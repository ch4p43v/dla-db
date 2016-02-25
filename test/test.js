var Database = require("../");
var DB = new Database({path: __dirname+'/db'});
DB.openSync();
DB.createTableSync('keywords');
DB.createColumnSync('keywords', 'word');
//DB.setIndexSync('keywords', 'word');

console.info(new Date().getTime());
var max = 1000;
var i = 0;
var loop = function() {
    if (i < max) {
        i++
        DB.findOne('keywords', null, {word: 'priority'}, function(err, data) {
            //console.info('.');
            //console.info(data);
            loop();
        });
    } else {
        console.info(new Date().getTime());
    }
};
loop();

// mit index
1455222225307
1455222543132


/*
DB.createTableSync('testtbl');
DB.createColumnSync('testtbl', 'word');
DB.insert('testtbl', {word: 'abc'}, function(err, ids) {
    DB.insert('testtbl', {word: 'öäüäääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääääää'}, function(err, ids) {
        DB.insert('testtbl', {word: 'xyz'}, function(err, ids) {
            console.info('ende');
        });        
    });    
});
*/