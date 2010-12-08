/*
 * Copyright (c) 2010 Digaku.com
 * writen by Robin Syihab (r[at]nosql.asia)
 *
 * License MIT
 *
 * Copyright (c) 2009 The Go Authors. All rights reserved.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package core

import (
	"fmt"
	"os"
	"net"
	//"strings"
	"mongo"
)

var (
	con *net.UDPConn
	db *mongo.Database
	colStream *mongo.Collection
	colPost *mongo.Collection
	colChan *mongo.Collection
	colTun *mongo.Collection
	colUser *mongo.Collection
)

const (
	CMD_QUIT = "/q"
	CMD_PROCESS = "/p" // /p [POST-ID]
)

var atreps map[string]string

type Settings struct {
	DbServer string
	DbPort int
}

type HasMetaname interface {
	Metaname() string
}

type SuperDoc struct {
	Id_ []byte
	Origin_id_ string
	Metaname_ string
}

type UserPost struct {
	Id_ []byte
	Origin_id_ string
	Metaname_ string
	Message string
}

type Channel struct{
	Id_ []byte
	Name string
	Desc string
	Metaname_ string
}

func (s *Channel) Metaname() string {return s.Metaname_;}

type User struct{
	Id_ []byte
	Name string
	Email_login string
	Lang_id string
	Desc string
	Metaname_ string
	Followed_user_ids_ []string
}
func (s *User) Metaname() string {return s.Metaname_;}

type Origin struct {
	Id_ []byte
	Name string
	Metaname_ string
}

type PostStream struct {
	UserId string
	PostId string
}

type searchPost struct {
	Id_ []byte
}

var settings *Settings;

func Init(_con *net.UDPConn, st *Settings){
	con = _con
	settings = st
	
	dbcon, err := mongo.Connect(st.DbServer,st.DbPort)
	if err != nil{fmt.Println("DB connection error.",err); return;}
	
	db = dbcon.GetDB("test")
	colStream = db.GetCollection("ndayak_streams")
	colPost = db.GetCollection("user_post")
	colChan = db.GetCollection("channel")
	colTun = db.GetCollection("tunnel")
	colUser = db.GetCollection("user")
	
	atreps = map[string]string{"_origin_id":"origin_id_","_metaname_":"metaname_","_followed_user_ids":"followed_user_ids_"}
}

func Worker(ch chan string){
	for {
		rv := <- ch
		//fmt.Println("worker received new task...")
		//fmt.Println("working...")
		
		switch rv[:2]{
		case CMD_PROCESS:
		 	post_id := rv[3:]
			
			fmt.Printf("Building index for post_id: %v...\n", post_id)
			process_post(post_id)
			
		case CMD_QUIT:
			fmt.Println("Quit command received. Realy quiting now...")
			con.Close()
			os.Exit(0)
		}
		//fmt.Println("worker finished task.")
	}
}

type oidSearch map[string]mongo.ObjectId

func getOrigin(originId string) (doc mongo.BSON, err os.Error){
	
	qfind, err := mongo.Marshal(oidSearch{"_id":mongo.ObjectId{originId}}, atreps)
	if err != nil{err = os.NewError("Cannot marshal"); return}
	
	doc, err = colUser.FindOne(qfind)
	if err != nil{
		doc, err = colChan.FindOne(qfind)
		if err != nil{
			doc, err = colTun.FindOne(qfind)
			if err != nil{
				err = os.NewError(fmt.Sprintf("Cannot find origin for id `%s`", originId))
				return
			}
		}
	}

	return doc, nil
}


func process_post(post_id string){
	
	// get post
	
	qfind, err := mongo.Marshal(oidSearch{"_id":mongo.ObjectId{post_id}}, atreps)
	if err != nil{fmt.Printf("Cannot marshal. %s\n", err); return;}

	doc, err := colPost.FindOne(qfind)
	if err != nil{
		fmt.Printf("Cannot find post by id `%s`. %s.\n",post_id,err)
		return
	}
	
	var post UserPost
	
	mongo.Unmarshal(doc.Bytes(), &post, atreps)
	
	fmt.Printf("Got post id: %v, origin: %s\n",post.Id_, post.Origin_id_)
	
	// get origin
	doc, _ = getOrigin(post.Origin_id_)
	
	var spdoc SuperDoc
	
	mongo.Unmarshal(doc.Bytes(), &spdoc, atreps)

	fmt.Printf("spdoc._metaname_: %s\n", spdoc.Metaname_)
	switch spdoc.Metaname_{
	case "User":
		var user User
		mongo.Unmarshal(doc.Bytes(), &user, atreps)
		fmt.Printf("user: %v\n", user)
		fmt.Printf("user._followed_user_ids: %v, len: %d\n", user.Followed_user_ids_, len(user.Followed_user_ids_))
	}

	// get users whos subscribe origin
	
	// insert post_id to user streams index
	
	//doc, _ := mongo.Marshal(map[string]string{
	//	"post_id": post_id
	//})
	
	//col.Insert(doc)
}

