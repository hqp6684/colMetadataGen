var csv = require("fast-csv");
var fs = require('fs');
var stream = fs.createReadStream("mtcars.csv");

csv
 .fromStream(stream,{headers : true})
 .on("data", function(data){
     console.log(Object.keys(data));
 })
 .on("end", function(){
     console.log("done");
 });
