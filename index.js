var _fs = require("fs"),
    _path = require("path"),
    _util = require("util");

module.exports = function(options) {
    this.path = __dirname;
    var _this = this;
    var _tables = {};
    var _handles = {};
    var _index = {};
    var _cache = {};
    var _autocloseInterval = null;

    var _handlesAutoclose = function() {
        var ts = new Date().getTime();
        for(var key in _handles) {
            if (ts - _handles[key].timestamp > 60*1000) {
                try {
                    _fs.closeSync(_handles[key].fd);
                    delete _handles[key];
                } catch(e) {}
            }
        }
    };
    
    var _defaultMetaData = function() { return {count: 0, columns: {}, free: []} };
    var _toPath = function(tableName) { return _this.path+_path.sep+tableName; };
    var _toMetaFile = function(tableName) { return _toPath(tableName)+_path.sep+'meta.json'; };
    var _getSector = function(tableName, colName, rowNum) {
        if (!_tables[tableName] || !_tables[tableName].columns[colName]) return null;
        
        var meta = _tables[tableName],
            col = meta.columns[colName];
            
        rowNum = parseInt(rowNum);
            
        return {
            start: col.length*rowNum + rowNum,
            end: col.length*rowNum + rowNum + col.length,
            length: col.length
        };
    };
    var _writeMetaFile = function(tableName, cb) {
        _fs.writeFile(_toMetaFile(tableName), JSON.stringify(_tables[tableName]), 'utf8', function(err2) {
            if (err2) cb(err2);
            else {
                cb(null);
            }
        });
    };
    var _writeMetaFileSync = function(tableName) {
        _fs.writeFileSync(_toMetaFile(tableName), JSON.stringify(_tables[tableName]), 'utf8');
    };

    var _match = function(val, match) {
        if (match instanceof Function) {
            return match(val);
        } 
        else if (match instanceof RegExp) {
            return (val+'').match(match);
        }
        else {
            return val == match;
        }
    };
    
    var _findColData = function(tableName, colName, value, ids, cb) {
        var meta = _tables[tableName],
            result = [],
            key = tableName+'|'+colName;

        if (!ids) {
            ids = [];
            for(var i=0; i < meta.count; i++) ids.push(i);
        }

        // find from cache
        if (_index[key] && (typeof value == 'string' || typeof value == 'number')) {
            if (_index[key]['$'+value]) {
                var cids = _index[key]['$'+value],
                    id;
                while(ids.length > 0) {
                    id = ids.shift();
                    if (cids.indexOf(id) > -1) result.push(id);
                }
            }

            cb(null, result);
            return;
        }

        var readLine = function() {
            if (ids.length > 0) {
                var sec = ids.shift();
                _read(tableName, colName, sec, function(err, data) {
                    if (err) {
                        cb(err, result);
                    } else {
                        if (_match(data, value)) result.push(sec);
                        readLine();
                    }
                });
            } else {
                cb(null, result);
            }
        };
        
        readLine();
    };
    
    var _toFieldValue = function(value, length, type) {
        var buf = new Buffer(length+1);
        buf.fill("\0");
        switch(typeof value) {
            case 'date':
                buf.write(value.toISOString(), 0, 'utf8');
                break;
            default:
                buf.write(value+'', 0, 'utf8');
                break;
        }
        buf.write("\n", length, 'utf8');

        /*var buf = new Buffer(value+'', 'utf8');
        if (buf.length > length) buf = buf.slice(0, length);

        var fill = '';
        for (var j = buf.length; j < length; j++) fill += "\0";
        fill += "\n";
        
        buf = buf.concat(fill);*/
        return buf.toString('utf8');
    };
    
    var _toColumnArray = function(tableName, columns) {
        if (!columns && columns !== 0) columns = '';
        
        if (typeof columns == 'string') {
            if (columns == '*' || columns === '') {
                columns = ['_id'];
                for (var cn in _tables[tableName].columns) columns.push(cn);
            } else {
                columns = [columns];
            }
        }
        return columns;
    };
    
    var _trim = function(val) {
        val += '';
        var start = val.indexOf("\0");
        return val.substr(0, start > -1 ? start : val.length);
    };
    
    var _decode = function(val, type) {
       val = _trim(val); 
       switch (type) {
           case 'int': val = !val || isNaN(val) ? 0 : parseInt(val); break;
           case 'number': case 'float': val = !val || isNaN(val) ? 0 : parseFloat(val); break;
       }
       
       return val;
    };
    
    var _open = function(tableName, colName, cb) {
        if (!_tables[tableName]) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        if (!_tables[tableName].columns[colName]) {
            cb(new Error('Column '+colName+' not exists in '+tableName+''));
            return;
        }
        var key = tableName+'|'+colName;
        if (_handles[key]) {
            _handles[key].timestamp = new Date().getTime();
            cb(null);
        } 
        else {
            var meta = _tables[tableName],
                col = meta.columns[colName],
                f = _toPath(tableName)+_path.sep+colName+'.'+col.type;

            _fs.open(f, 'r+', function(err, fd) {
                if (!err) {
                    _handles[key] = {
                        tableName: tableName,
                        colName: colName,
                        fd: fd,
                        timestamp: new Date().getTime()
                    };
                }
                
                cb(err);
            });
        }
    };
    
    var _openSync = function(tableName, colName) {
        if (!_tables[tableName]) throw new Error('Table '+tableName+' not exists');
        if (!_tables[tableName].columns[colName]) throw (new Error('Column '+colName+' not exists in '+tableName+''));
        
        var key = tableName+'|'+colName;
        if (_handles[key]) {
            _handles[key].timestamp = new Date().getTime();
        } 
        else {
            var meta = _tables[tableName],
                col = meta.columns[colName],
                f = _toPath(tableName)+_path.sep+colName+'.'+col.type;
                
            var fd = _fs.openSync(f, 'r+');
            _handles[key] = {
                tableName: tableName,
                colName: colName,
                fd: fd,
                timestamp: new Date().getTime()
            };
        }
    };
    
    var _read = function(tableName, colName, id, cb) {
        var key = tableName+'|'+colName;

        _open(tableName, colName, function(err) {
            if (err) {
                cb(err, null);
            }
            else {
                var col = _tables[tableName].columns[colName],
                    buffer = new Buffer(col.length),
                    sector = _getSector(tableName, colName, id);

                if (_handles[key]) {
                    var fd = _handles[key].fd;
                    _fs.read(fd, buffer, 0, buffer.length, sector.start, function(err2) {
                        if (err2) {
                                cb(err2, null);
                        } else {
                            var data = _decode(buffer.toString('utf8'), col.type);
                            cb(null, data);
                        }
                    });
                }
                else {
                    _read(tableName, colName, id, cb);
                }
            }
        });
    };
    
    var _readSync = function(tableName, colName, id) {
        var key = tableName+'|'+colName;

        _openSync(tableName, colName);
        var col = _tables[tableName].columns[colName],
            buffer = new Buffer(col.length),
            sector = _getSector(tableName, colName, id);

        if (_handles[key]) {
            var fd = _handles[key].fd;
            _fs.readSync(fd, buffer, 0, buffer.length, sector.start);
            return _decode(buffer.toString('utf8'), col.type);
        }
        else {
            return _readSync(tableName, colName, id);
        }
    };

    var _write = function(tableName, colName, id, data, cb) {
        _open(tableName, colName, function(err) {
            if (err) {
                cb(err);
            } 
            else {
                var col = _tables[tableName].columns[colName],
                    buffer,
                    fd,
                    sector = _getSector(tableName, colName, id),
                    key = tableName+'|'+colName;
                
                if (_handles[key]) {
                    fd = _handles[key].fd;

                    // indexing
                    if (_index[key]) {
                        if (!_index[key]['$'+data]) _index[key]['$'+data] = [];
                        _index[key]['$'+data].push(id);
                    }
                    
                    buffer = new Buffer(_toFieldValue(data, col.length, col.type));
                        
                    _fs.write(fd, buffer, 0, buffer.length, sector.start, function(err2) {
                        cb(err2);
                    }); 
                }
                else {
                    _write(tableName, colName, id, data, cb);
                }
            }
        });
    };
    
    var _writeSync = function(tableName, colName, id, data) {
        _openSync(tableName, colName);
        var col = _tables[tableName].columns[colName],
            buffer,
            fd,
            sector = _getSector(tableName, colName, id),
            key = tableName+'|'+colName;
        
        if (_handles[key]) {
            fd = _handles[key].fd;
            buffer = new Buffer(_toFieldValue(data, col.length, col.type));
                
            _fs.writeSync(fd, buffer, 0, buffer.length, sector.start); 
        }
        else {
            _write(tableName, colName, id, data, cb);
        }
    };
    
    this.stat = function(tableName, cb) {
        var p = _toMetaFile(tableName);
        _fs.stat(p, function(err, Stats) {
            if (err) cb(err, null);
            else {
                _fs.readFile(p, {encoding: 'utf8'}, function(err2, meta) {
                    if (err2) cb(err2, null);
                    else {
                        try {
                            cb(null, JSON.parse(meta));  
                        } catch(e) {
                            cb(e, null);    
                        }
                    }
                });
            }
        });
    };

    this.statSync = function(tableName) {
        var p = _toMetaFile(tableName);
        var meta = _fs.readFileSync(p, {encoding: 'utf8'});
        return JSON.parse(meta);  
    };
    
    this.getTables = function() {
        return _tables;
    };
    
    this.tableExists = function(tableName) {
        return _tables[tableName] ? true : false;
    };
    
    this.columnExists = function(tableName, colName) {
        return _tables[tableName] && _tables[tableName].columns[colName];
    };

    /**
     * Open Database
     * @param {Function} cb
     */
    this.open = function(cb) {
        if (!_autocloseInterval) _autocloseInterval = setInterval(_handlesAutoclose, 60*1000);
        
        _fs.readdir(this.path, function(err, items) {
            if (err) {
                _fs.mkdir(_this.path, function(err1) {
                    cb(err);
                });
                return;
            }
            var findDatabase = function() {
                if (items.length > 0) {
                    var item = items.shift();
                    _fs.stat(_this.path+_path.sep+item, function(err, Stats) {
                       if (!err && Stats.isDirectory()) {
                           _fs.readFile(_this.path+_path.sep+item+_path.sep+'meta.json', {encoding: 'utf8'}, function(err, MetaJson) {
                               if (!err) {
                                    try {
                                        _tables[item] = JSON.parse(MetaJson);
                                        
                                    } catch(e) {}
                                    
                                }
                                findDatabase();
                           });
                       } else {
                           findDatabase();
                       }
                    });
                } else {
                    cb(err);
                }
            };
            
            findDatabase();
        });
    };
    
    /**
     * Open Database Schnchron
     */
    this.openSync = function() {
        if (!_autocloseInterval) _autocloseInterval = setInterval(_handlesAutoclose, 60*1000);
        
        try {
            var items = _fs.readdirSync(this.path);
            while (items.length > 0) {
                var item = items.shift();
                try {
                    var Stats = _fs.statSync(_this.path+_path.sep+item);
                    if (Stats.isDirectory()) {
                        var MetaJson = _fs.readFileSync(_this.path+_path.sep+item+_path.sep+'meta.json', {encoding: 'utf8'});
                        _tables[item] = JSON.parse(MetaJson);
                    }
                } catch (e) {}
            }
        } catch (err) {
            _fs.mkdirSync(_this.path);
        }
    };
    
    /**
     * Close Database
     */
    this.close = function() {
        if (_autocloseInterval) {
            clearInterval(_autocloseInterval);
            _autocloseInterval = null;
            for(var key in _handles) {
                try {
                    _fs.closeSync(_handles[key].fd);
                } catch(e) {}
                delete _handles[key];
            }
        }
    };
    
    /*this.setCache = function(tableName, colName, cacheLimit) {
        var key = tableName+'|'+colName;
        cacheLimit
    };*/
    
    /**
     * @param {String} tableName
     * @param {Function} cb
     */
    this.createTable = function(tableName, cb) {
        this.stat(tableName, function(err, Stats) {
            if (err) {
                var p = _toPath(tableName);
                _fs.mkdir(p, function(err1) {
                    if (err1 && err1.code != 'EEXIST') cb(err1, null);
                    else {
                        _tables[tableName] = _defaultMetaData();
                        _writeMetaFile(tableName, function(metaErr) {
                            cb(metaErr);
                        });
                    }
                });
            }
            else cb(null, Stats);
        });
    };
    
    /**
     * @param {String} tableName
     */
    this.createTableSync = function(tableName) {
        try {
            return this.statSync(tableName);
        } 
        catch(err) {
            var p = _toPath(tableName);
            try {
                _fs.mkdirSync(p);
                _tables[tableName] = _defaultMetaData();
                _writeMetaFileSync(tableName);
            } catch(err1) {
                if (err1.code != 'EEXIST') throw err1;
            }
        }
    };
    
    /**
     * @param {String} tableName
     * @param {Object|String} o Options {name:tablename, type:string, length: 255} or Name
     * @param {Function} cb
     */
    this.createColumn = function(tableName, o, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table not exists'));
            return;
        }
        
        if (typeof o == 'string') o = {name: o};
        if (!o.type) o.type = 'string';
        if (!o.length) o.length = 128;
        
        var meta = _tables[tableName];
        if (meta.columns[o.name]) {
             cb(null);
             return;
        }

        var f = _toPath(tableName)+_path.sep+o.name+'.'+o.type;
        
        _fs.stat(f, function(err, Stats) {
            if (err) {
                _fs.writeFile(f, '', function(err2) {
                    if (err2) {
                        cb(err2);
                        return false;
                    }
                    
                    var id = -1;
                    meta.columns[o.name] = {type: o.type, length: o.length};

                    var writeLoop = function() {
                        id++;
                        if (id < meta.count) {
                            _write(tableName, o.name, id, '', function(err3) {
                                if (err3) {
                                     _writeMetaFile(tableName, function(err4) {
                                        cb(err3);
                                    });
                                }
                                else {
                                    writeLoop();
                                }
                            });
                        } else {
                             _writeMetaFile(tableName, function(err4) {
                                cb(null);
                            });
                        }
                    };
                    
                    writeLoop();
                });
            } else {
                _tables[tableName].columns[o.name] = {type: o.type, length: o.length};
                _writeMetaFile(tableName, function(err2) {
                    cb(err2);
                });
            }
        });
    };
    
    /**
     * @param {String} tableName
     * @param {Object|String} o Options {name:tablename, type:string, length: 255} or Name
     * @param {Function} cb
     */
    this.createColumnSync = function(tableName, o) {
        if (!this.tableExists(tableName)) throw new Error('Table not exists');
        
        if (typeof o == 'string') o = {name: o};
        if (!o.type) o.type = 'string';
        if (!o.length) o.length = 128;
        
        var meta = _tables[tableName];
        if (meta.columns[o.name]) return;

        var f = _toPath(tableName)+_path.sep+o.name+'.'+o.type;
        try {
            var Stats = _fs.statSync(f);

            _tables[tableName].columns[o.name] = {type: o.type, length: o.length};
            _writeMetaFileSync(tableName);
        } catch (e) {
            _fs.writeFileSync(f, '');
            var id = 0;
            meta.columns[o.name] = {type: o.type, length: o.length};

            try {
                while (id < meta.count) {
                    _writeSync(tableName, o.name, id, '');
                    id++;
                }
            } catch(e) {
                _writeMetaFile(tableName);
                throw e;
            }
        }
    };
    
    this.setIndex = function(tableName, colName, cb) {
        if (!this.tableExists(tableName)) cb(new Error('Table '+tableName+' does not exists'));
        else if (!this.columnExists(tableName, colName)) cb(new Error('column '+tableName+'.'+colName+' does not exists'));
        else {
            var key = tableName+'|'+colName;
            var errors = [];
        
            if (_index[key]) cb(null);
            else {
                _index[key] = {};
                var col = _tables[tableName].columns[colName];
                var i = -1;
                
                var indexLoop = function() {
                    i++;
                    if (i < _tables[tableName].count) {
                        _read(tableName, colName, i, function(err, data) {
                            if (err) errors.push(err);
                            else {
                                if (data && data.length > 0) {
                                    if (!_index[key]['$'+data]) _index[key]['$'+data] = [];
                                    _index[key]['$'+data].push(i);
                                }
                            }
                            indexLoop();
                        });
                    } else {
                        cb(errors.length > 0 ? errors : null);
                    }
                };
                
                indexLoop();
            }
        }
    };
    
    this.setIndexSync = function(tableName, colName) {
        if (!this.tableExists(tableName)) return new Error('Table '+tableName+' does not exists');
        else if (!this.columnExists(tableName, colName)) return new Error('column '+tableName+'.'+colName+' does not exists');
        else {
            var key = tableName+'|'+colName;

            if (!_index[key]) {
                _index[key] = {};
                for(var i=0; i < _tables[tableName].count; i++) {
                    var data = _readSync(tableName, colName, i);
                    if (data && data.length > 0) {
                        if (!_index[key]['$'+data]) _index[key]['$'+data] = [];
                        _index[key]['$'+data].push(i);
                    }
                }
            }
        }
    };
    
    /**
     * @param {String} tableName
     * @param {String} colName
     * @param {Array} ids
     * @param {Function} cb
     */
    this.getValuesByID = function(tableName, colName, ids, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        if (!this.columnExists(tableName, colName)) {
            cb(new Error('Column '+colName+' not exists in '.tableName));
            return;
        }
        
        if (!(ids instanceof Array)) ids = [ids];
        
        var result = [];
        var idsClone = ids.slice(0);
        
        var readLoop = function() {
            if (idsClone.length > 0) {
                var sec = idsClone.shift();
                _read(tableName, colName, sec, function(err2, data) {
                    if (err2) {
                        cb(err2, result);
                    } else {
                        result.push(data);
                        readLoop();
                    }
                });
                
            }
            else {
                cb(null, result);
            }
        };
        
        readLoop();
    };
    
    /**
     * @param {String} tableName
     * @param {Array|Number} ids
     * @param {Array} columns
     * @param {Function} cb
     */
    this.getDataByID = function(tableName, ids, columns, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
       
        columns = _toColumnArray(tableName, columns);
        
        var tmpResult = {};
        var result = [];
        var useID = columns.indexOf('_id') > -1;
        if (!(ids instanceof Array)) ids = [ids];

        for(var i = 0; i < columns.length; i++) tmpResult[columns[i]] = [];
        
        var getLoop = function() {
            if (columns.length > 0) {
                var colName = columns.shift();
                if (colName == '_id') {
                    getLoop();
                    return;
                }
                _this.getValuesByID(tableName, colName, ids, function(err, values) {
                    if (err) {
                        for(var i = 0; i < ids.length; i++) tmpResult[colName].push(null);
                    } else {
                        tmpResult[colName] = values;
                    }
                    getLoop();
                });
            }
            else {
                for(var i=0; i < ids.length; i++) {
                    var row = {};
                    if (useID) row._id = ids[i];
                    for(var n in tmpResult) {
                        if (n == '_id') continue;
                        row[n] = tmpResult[n].shift();
                    }
                    
                    result.push(row);
                }
                
                cb(null, result);
            }
        };
        
        getLoop();
       
    };
    
    /**
     * @param {String} tableName
     * @param {String} colName
     * @param {Array} data {id: value}
     * @param {Function} cb
     */
    this.setColumnValuesByID = function(tableName, colName, data, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        if (!this.columnExists(tableName, colName)) {
            cb(new Error('Column '+colName+' not exits in table '+tableName+''));
            return;
        }
        
        var ids = Object.keys(data),
            errors = [];
            
        var writeLoop = function() {
            if (ids.length > 0) {
                var rowNum = ids.shift();
                
                _write(tableName, colName, rowNum, data[rowNum], function(err2) {
                    if (err2) errors.push(err2);
                    writeLoop();
                });
            } else {
                cb(errors.length > 0 ? errors : null);
            }
        };
        
        writeLoop();
    };
    
    /**
     * @param {String} tableName
     * @param {Object} data {id: {fieldname:fieldvalue}}
     * @param {Function} cb
     */
    this.setDataByID = function(tableName, data, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        var colData = {},
            columns = [],
            errors = [];
            
        for(var id in data) {
            for(var col in data[id]) {
                if (!colData[col])  {
                    colData[col] = {};
                    columns.push(col);
                }
                colData[col][id] = data[id][col];
            }
        }
        
        var writeColLoop = function() {
            if (columns.length > 0) {
                var colName = columns.shift();
                _this.setColumnValuesByID(tableName, colName, colData[colName], function(err) {
                    if (err) errors.push(err);
                    delete colData[colName];
                    writeColLoop();
                });
            } else {
                cb(errors.length > 0 ? errors : null);
            }
        };
        
        writeColLoop();
    };
    
    /**
     * @param {String} tableName
     * @param {Object} whereData {fieldname:fieldvalue}
     * @param {Function} cb
     */
    this.findIDs = function(tableName, whereData, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }

        var fields = Object.keys(whereData),
            ids = whereData._id ? (_util.isArray(whereData._id) ? whereData._id : [whereData._id]) : null;

        var whereloop = function() {
            if (fields.length > 0) {
                 var field = fields.shift();
                if (field == '_id') {
                    whereloop();
                    return;
                }
        
                _findColData(tableName, field, whereData[field], ids, function(err, result) {
                    if (err) 
                        whereloop();
                    else {
                        if (result && result.length > 0) {
                            ids = result;
                            whereloop();
                        }
                        else {
                            cb(ids);
                        }
                    }
                });

            }
            else {
                cb(ids);
            }
        };
        
        whereloop();
    };
    
    /**
     * @param {String} tableName
     * @param {Array} columns Column name array
     * @param {Object} whereData {fieldname:fieldvalue}
     * @param {Function} cb
     */
    this.find = function(tableName, columns, whereData, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        this.findIDs(tableName, whereData, function(ids) {
            _this.getDataByID(tableName, ids, columns, cb);
        });
    };
    
    this.findOne = function(tableName, columns, whereData, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        this.findIDs(tableName, whereData, function(ids) {
            if (ids && ids.length > 0) {
                _this.getDataByID(tableName, ids.shift(), columns, function(err, data) {
                    cb(err, data && data.length > 0 ? data.shift() : null);
                });
            } else {
                cb(null, null);
            }
        });
    };
    
    /**
     * @param {String} tableName
     * @param {Object} whereData {fieldname:fieldvalue}
     * @param {Function} cb
     */
    this.remove = function(tableName, whereData, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        var meta = _tables[tableName],
            col,
            iData = {};
            
        for(var colName in meta.columns) {
            col = meta.columns[colName];
            iData[colName] = _toFieldValue("", col.length, col.type);
        }
        
        this.update(tableName, iData, whereData, function(err, ids) {
            for(var i = 0; i < ids.length; i++) meta.free.push(ids[i]);
            
            _writeMetaFile(tableName, function(metaErr) {
                cb(err, ids);
            });
        });
    };
    
    /**
     * @param {String} tableName
     * @param {Object} updateData {fieldname:fieldvalue}
     * @param {Object} whereData {fieldname:fieldvalue}
     * @param {Function} cb
     */
    this.update = function(tableName, updateData, whereData, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        var meta = _tables[tableName],
            col,
            id,
            resultIDs = [];
        
        this.findIDs(tableName, whereData, function(ids) {
            if (ids && ids.length > 0) {
                var iData = {};
                while(ids.length > 0) {
                    id = ids.shift();
                    for(var colName in updateData) {
                        col = meta.columns[colName];
                        if (!iData[id]) iData[id] = {};
                        if (col) {
                            iData[id][colName] = _toFieldValue(updateData[colName], col.length, col.type);
                        }
                        resultIDs.push(id);
                    }
                    
                    _writeMetaFile(tableName, function(metaErr) {
                        _this.setDataByID(tableName, iData, function(err) {
                            cb(err, resultIDs);
                        });
                    });
                }
            }
        });
    };
    
    /**
     * @param {String} tableName
     * @param {Object} replaceData {fieldname:fieldvalue}
     * @param {Object} whereData {fieldname:fieldvalue}
     * @param {Function} cb
     */
    this.replace = function function_name(tableName, replaceData, whereData, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        var uData = {};
        for(var colName in _tables[tableName].columns) {
            uData = typeof replaceData[colName] == 'undefined' ? '' : replaceData[colName];
        }
        
        this.update(tableName, uData, whereData, cb);
    };
    
    /**
     * @param {String} tableName
     * @param {Array|Object} data {fieldname:fieldvalue}
     * @param {Function} cb Returns IDs (line numbers from 0)
     */
    this.insert = function(tableName, data, cb) {
        if (!this.tableExists(tableName)) {
            cb(new Error('Table '+tableName+' not exists'));
            return;
        }
        
        var meta = _tables[tableName];
        
        if (!(data instanceof Array)) {
            data = [data];
        }
        
        var iData = {},
            rowNum;

        for(var i = 0; i < data.length; i++) {
            if (meta.free.length > 0) {
                rowNum = meta.free.shift();
            } else {
                rowNum = meta.count;
                meta.count++;
            }
            iData[rowNum] = data[i];
        }
        
        _writeMetaFile(tableName, function(mErr) {
            if (mErr) cb(mErr);
            else {
                _this.setDataByID(tableName, iData, function(err) {
                    cb(err, Object.keys(iData));
                });
            }
        });
    };
    
    if (options) for(var i in options) this[i] = options[i];
};
