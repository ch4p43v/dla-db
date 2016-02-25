# dla-db
NodeJS file Based Database. Simple and very fast. Simple to embed in your application. Current version is beta

#### TODO for v1.0.0:
  - find witch _id
  - more data types (at the moment only string, int and float)
  - Sych methodes

#### Cahcnelog:
```
v0.92.0
+ _id as Where condition

v0.91.0
- BUG Indexing
~ Speedup indexed columns

v0.90.0
+ openSync function
+ createTableSync function
+ createColumnSync function
+ setIndex function (indexed columns)
+ setIndexSync function (indexed columns)
+ int column type
+ float column type
- BUG UTF-8 values
```

Database is a folder table is subfolder and column is file
  - Testdatabase
     - Testtable
        - meta.json
        - testcol.string
        - testcol.int
# Installation
```sh
npm install --save dla-db
```

# Usage
Create table
```javascript
var Database = require('dla-db');
var MyDB = new Database({path: '/path/to/database/root/folder'});

// MyDB.openSync() or
MyDB.open(function() {
    // Create Table
    // MyDB.createTableSync('testtable') or
    MyDB.createTable('testtable', function(tErr) {
        if (tErr) {
            console.error(tErr);
        } else {
            var colSettings = {name: 'testcol', type: 'string', length: 255};
            // Table is created
            // Create column now
            // MyDB.createColumnSync('testtable', colSettings) or
            MyDB.createColumn('testtable', colSettings, function(cErr) {
            if (cErr) {
                console.error(cErr);
            } else {
                // column ist created
                // do something
            });
        }
    });
});
```

Insert data
```javascript
// single insert
MyDB.insert('testtable', {'testcol': 'testvalue1'}, function(err, ids) {
    console.info(arguments);
});

// multiple insert
MyDB.insert('testtable', [{'testcol': 'testvalue2'}, {'testcol': 'testvalue3'}], function(err, ids) {
    // id is line number from 0
    console.info(arguments);
});
```

Select data by value
```javascript
var resultfields = ['_id', 'testcol'];
// find by value
var whereCondition = {testcol: 'testvalue2'};
MyDB.find('testtable', resultfields, whereCondition, function() {
   console.info(arguments);
});

// find by regexp
var whereCondition = {testcol: /testvalue/gi};
MyDB.find('testtable', resultfields, whereCondition, function() {
   console.info(arguments);
});

// find by own condition ;-)
var whereCondition = {testcol: function(val) { return val < 10; }};
MyDB.find('testtable', resultfields, whereCondition, function() {
   console.info(arguments);
});
```

Update and Replace
```javascript
var updateData = {testcol: 'testvalue4'};
var whereData = {testcol: 'testvalue3'};
// update replace only selected columns
MyDB.update('testtable', updateData, whereData, function(err, ids) {
    console.info(arguments);
});

// replace will replace all columns
MyDB.replace('testtable', updateData, whereData, function(err, ids) {
    console.info(arguments);
});
```

Remove data
```javascript
var whereData = {testcol: 'testvalue4'};
MyDB.remove('testtable', whereData, function(err, ids) {
    console.info(arguments);
});
```
