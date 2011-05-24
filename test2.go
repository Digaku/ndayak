
package main


import (
	"reflect"
	"fmt"
	"launchpad.net/gobson/bson"
    "launchpad.net/mgo"
	"encoding/hex"
)


type User struct {
	Name string
	Email_login string
	_metaname_ string
}


type SuperDocIface interface {
	GetInt64() int64
	GetString() string
}

type SuperDoc struct {
	val map[string]interface{}
}

func NewSuperDoc(d map[string]interface{}) *SuperDoc {
	return &SuperDoc{d}
}

func (self SuperDoc) GetInt(k string) int64 {
	v := reflect.ValueOf(self.val[k])
	switch v.Kind() {
	case reflect.Int64, reflect.Int32, reflect.Int:
		return v.Int()
	}
	return 0
}



func (self SuperDoc) GetString(k string) string {
	//fmt.Printf("self.val.Kind() : %s\n", self.val.Kind())
	v := reflect.ValueOf(self.val[k])
	if v.Kind() == reflect.String {
		if k == "_id" {
			return hex.EncodeToString([]byte(string(v.String())))
		}
		return v.String()
	}
	return ""
}

func main() {
        session, err := mgo.Mongo("localhost")
        if err != nil {
                panic(err)
        }
        defer session.Close()

        // Optional. Switch the session to a monotonic behavior.
        session.SetMode(mgo.Monotonic, true)

        c := session.DB("digaku").C("user")

        var result map[string]interface{}

        err = c.Find(bson.M{"name": "robin"}).One(&result)
        if err != nil {
			panic(err)
        }

		//fmt.Println("typeof result['_id'] = ", reflect.ValueOf(result["_id"]).Kind())
		//fmt.Println("typeof result['_metaname_'] = ", reflect.ValueOf(result["_metaname_"]).Kind())
		//fmt.Println("typeof result['_pass_v'] = ", reflect.ValueOf(result["_pass_v"]).Kind())

		//var _id string
		
		//if v, ok := result["_id"].(bson.ObjectId); ok{
		//	_id = v.ToString()
		//}
		
		doc := NewSuperDoc(result)

        fmt.Printf("ID: %s, User: %v, email: %v, metaname: %v, _pass_v: %v\n", 
			doc.GetString("_id"),
			doc.GetString("name"), 
			doc.GetString("email_login"), 
			doc.GetString("_metaname_"),
			doc.GetInt("_pass_v"))
}




