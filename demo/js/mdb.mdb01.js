
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/mydb";

MongoClient.connect(url, function(err, db) {
  if (err) throw err;
  console.log("Database connected.");
/*
  db.createCollection("customers", function(err, res) {
    console.log("Collection created!");
    db.close();
  });
  */
  db.collections((collections) => {
      console.log("Collections:", collections);
      db.close();
    });
});

