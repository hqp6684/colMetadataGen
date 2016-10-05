var fs = require('fs')
    , es = require('event-stream');

var lineNr = 0;
var metaData = {};

var s = fs.createReadStream('./mtcars.csv')
    .pipe(es.split())

    .pipe(es.mapSync(function(line){

        // pause the readstream
        s.pause();


    if(lineNr==0){
        var dataArray = csvToArray(line);
        dataArray.map(function(value,index){
            metaData[index] = new MetaDataCount();
        })
        lineNr +=1;

    }else{
        var dataArray = csvToArray(line);
        dataArray = csvToArray(line);
        dataArray.map(function(data,index){
            if(data){
                if(!isNaN(data)){metaData[index].numericCount += 1; return};
                if(new Date(data) == 'Invalid Date'){
                    console.log(data +" is not date");
                    metaData[index].factorCount +=1;
                    return;
                }else{ console.log(data + ' is date') ;metaData[index].dateCount +=1; return;}

            }else{
                // metaData[index].emptyCount += 1;
                return;
            }
        })
    }
        // process line here and call s.resume() when rdy
        // function below was for logging memory usage
        // logMemoryUsage(lineNr);

        // resume the readstream, possibly from a callback
        s.resume();
    })
    .on('error', function(){
        console.log('Error while reading file.');
    })
    .on('end', function(){
        console.log('Read entire file.');
        var report = {};
        for(var col in metaData){
            // console.log(col);
            var highest = 0;
            var type = '';
            for(var countType in metaData[col]){
                if(metaData[col][countType] > highest){

                    higest =  metaData[col][countType];
                    type = countType;
                }
            }
            report[col] = type;

        }

        fs.writeFile('./report.txt', JSON.stringify(report));
        console.log(metaData);
    })
);

// http://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript-which-contains-comma-in-data
var csvToArray = function (text) {
    var re_valid = /^\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*(?:,\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*)*$/;
    var re_value = /(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g;
    // Return NULL if input string is not well formed CSV string.
    if (!re_valid.test(text)) return null;
    var a = [];                     // Initialize array to receive values.
    text.replace(re_value, // "Walk" the string using replace with callback.
        function(m0, m1, m2, m3) {
            // Remove backslash from \' in single quoted values.
            if      (m1 !== undefined) a.push(m1.replace(/\\'/g, "'"));
            // Remove backslash from \" in double quoted values.
            else if (m2 !== undefined) a.push(m2.replace(/\\"/g, '"'));
            else if (m3 !== undefined) a.push(m3);
            return ''; // Return empty string.
        });
    // Handle special case of empty last value.
    if (/,\s*$/.test(text)) a.push('');
    return a;
};

function MetaDataCount(){
    // this.emptyCount = 0;
    this.dateCount = 0;
    this.numericCount = 0;
    this.factorCount=0;
}