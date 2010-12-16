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

package ndayak

import (
	"fmt"
	"net"
	"os"
	"mongo"
)

var (
	con net.PacketConn
	db *mongo.Database
	dbcon *mongo.Connection
)

var atreps map[string]string
type oidSearch map[string]mongo.ObjectId

type Settings struct {
	DbServer string
	DbPort int
	DbName string
}

var settings *Settings;

func Init(_con net.PacketConn, st *Settings, verbosity int){
	con = _con
	settings = st
	
	SetVerbosity(verbosity)
	
	var err os.Error
	
	dbcon, err = mongo.Connect(st.DbServer,st.DbPort)
	if err != nil{fmt.Println("DB connection error.",err); return;}

	db = dbcon.GetDB(st.DbName)
	ColStream = db.GetCollection("ndayak_streams")
	ColPost = db.GetCollection("user_post")
	ColChan = db.GetCollection("channel")
	ColTun = db.GetCollection("tunnel")
	ColUser = db.GetCollection("user")
	
	ColStream.EnsureIndex("ndayax_1",map[string]int{"userid":1,"postid":1})
	
	atreps = map[string]string{
		"_origin_id":"origin_id_",
		"_metaname_":"metaname_",
		"_followed_user_ids":"followed_user_ids_",
		"_writer_id":"writerid",
		"_popular_post":"popular_post_",
	}
}

func ProcessPost(post_id string){

	if dbcon == nil { Error("Database not connected.\n"); return;}

	// get post
	
	qfind, err := mongo.Marshal(oidSearch{"_id":mongo.ObjectId{post_id}}, atreps)
	if err != nil{Error("Cannot marshal. %s\n", err); return;}

	doc, err := ColPost.FindOne(qfind)
	if err != nil{
		Warn("Cannot find post by id `%s`. %s.\n",post_id,err)
		return
	}
	
	
	var post UserPost
	
	mongo.Unmarshal(doc.Bytes(), &post, atreps)
	
	Info2("Got post id: %v, writer: %s, origin: %s\n",strid(post.Id_), post.WriterId, post.Origin_id_)
	
	// get writer
	writer, err := GetUser(post.WriterId)
	if err != nil{
		Warn("Cannot get writer with id `%s` for post id `%s`. err: %v\n", post.WriterId, strid(post.Id_), err)
		return
	}
	
	//fmt.Printf("writer: %v\n", writer.Name)
	InsertPostStream(strid(writer.Id_), post_id)
	
	var writer_id string = strid(writer.Id_)
	//fmt.Printf("writer_id: %s\n", writer_id)
	
	// get all followers
	qfind, err = mongo.Marshal(map[string]string{"_followed_user_ids":writer_id}, atreps)
	if err != nil{Error("Cannot marshal. %s\n", err); return;}

	// broadcast to all followers
	cursor, err := ColUser.FindAll(qfind)
	if err == nil{
		for cursor.HasMore(){
			doc, err = cursor.GetNext()
			if err != nil{Error("Cannot get next. e: %v\n", err); break}
		
			var follower User
			mongo.Unmarshal(doc.Bytes(), &follower, atreps)
			Info2("broadcast to follower: id: %v, name: %v\n", strid(follower.Id_), follower.Name)
		
			// insert to follower streams
			InsertPostStream(strid(follower.Id_), post_id)
		}
	}else{
		Warn("Cannot find post by id `%s`. %s.\n",post_id,err)
	}
	
	
	// get origin
	doc, err = GetOrigin(post.Origin_id_)
	if err == nil{

		var spdoc SuperDoc

		mongo.Unmarshal(doc.Bytes(), &spdoc, atreps)

		switch spdoc.Metaname_{
		case "Channel":
			var ch Channel
			mongo.Unmarshal(doc.Bytes(), &ch, atreps)

			//var ch_id string = strid(ch.Id_)
			// fmt.Printf("ch_id: %s\n", ch_id)

			// get all members
			qfind, err := mongo.Marshal(map[string]string{"_channel_ids":strid(ch.Id_)}, atreps)
			if err != nil{Error("Cannot marshal. %s\n", err); return;}
			
			// broadcast to all members
			cursor, err := ColUser.FindAll(qfind)
			if err == nil{
				for cursor.HasMore(){
					doc, err = cursor.GetNext()
					if err != nil{Error("Cannot get next. e: %v\n", err); break}

					var follower User
					mongo.Unmarshal(doc.Bytes(), &follower, atreps)
					Info2("broadcast to member: id: %v, name: %v\n", strid(follower.Id_), follower.Name)

					// insert to follower streams
					InsertPostStream(strid(follower.Id_), post_id)
				}
			}else{
				Warn("Cannot find post by id `%s`. %s.\n",post_id,err)
			}
		}

	}else{
		Warn("Cannot get origin id `%s`\n", post.Origin_id_)
	}
	
}


func TopUpPost(postId string) {
	if dbcon == nil { Error("Database not connected.\n"); return;}
	RmPostStream(postId)
	
	post, err := GetPost(postId)
	if err != nil{
		Error("TopUpPost: post doesn't exists for id `%v`\n", postId)
		return
	}
	
	Info2("TopUpPost: got post with id `%v`\n", strid(post.Id_))
	
	qfind, err := mongo.Marshal(oidSearch{"_id":mongo.ObjectId{postId}}, atreps)
	if err != nil{Error("TopUpPost: Cannot marshal. %s\n", err); return;}
	
	doc, err := mongo.Marshal(map[string]map[string]bool{"$set":{"_popular_post":true}}, atreps)
	if err != nil{Error("TopUpPost: Cannot marshal. %s\n", err); return;}
	
	err = ColPost.Update(qfind, doc);
	if err != nil{Error("TopUpPost: Cannot update.\n"); return;}
	
	BroadcastAll(postId)
}

func BroadcastAll(postId string) {

	if dbcon == nil { Error("Database not connected.\n"); return;}

	// get post
	
	qfind, err := mongo.Marshal(oidSearch{"_id":mongo.ObjectId{postId}}, atreps)
	if err != nil{Error("Cannot marshal. %s\n", err); return;}

	doc, err := ColPost.FindOne(qfind)
	if err != nil{
		Warn("Cannot find post by id `%s`. %s.\n",postId,err)
		return
	}
	
	var post UserPost
	
	mongo.Unmarshal(doc.Bytes(), &post, atreps)
	
	Info2("Got post id: %v, writer: %s, origin: %s\n",strid(post.Id_), post.WriterId, post.Origin_id_)
	
	// rm old refs
	RmPostStream(postId)
	
	// broadcast to all users
	qfind, err = mongo.Marshal(map[string]string{}, atreps)
	if err != nil{Error("Cannot marshal. %s\n", err); return;}
	
	cursor, err := ColUser.FindAll(qfind)
	if err == nil{
		for cursor.HasMore(){
			doc, err = cursor.GetNext()
			if err != nil{Error("Cannot get next. e: %v\n", err); break}
		
			var follower User
			mongo.Unmarshal(doc.Bytes(), &follower, atreps)
			Info2("broadcast to all: id: %v, name: %v\n", strid(follower.Id_), follower.Name)
		
			// insert to follower streams
			InsertPostStream(strid(follower.Id_), postId)
		}
	}else{
		Warn("Cannot find post by id `%s`. %s.\n",postId,err)
	}
	
}
