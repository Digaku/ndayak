package main

import (
	"github.com/garyburd/go-mongo"
	"log"
)

type TestDoc struct {
    Id    mongo.ObjectId "_id"
    Title string
    Body  string
}



func main() {

    // Connect to server.

    conn, err := mongo.Dial("localhost")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    c := mongo.Collection{conn, "test.test", mongo.DefaultLastErrorCmd}

    // Insert a document.

    id := mongo.NewObjectId()

    err = c.Insert(&TestDoc{Id: id, Title: "Hello", Body: "Mongo is fun."})
    if err != nil {
        log.Fatal(err)
    }

    // Find the document.

    var doc TestDoc
    err = c.Find(map[string]interface{}{"_id": id}).One(&doc)
    if err != nil {
        log.Fatal(err)
    }

    log.Print(doc.Title, " ", doc.Body)
}



