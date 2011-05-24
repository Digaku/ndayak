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
	"launchpad.net/gobson/bson"
    "launchpad.net/mgo"
	"strings"
)

var (
	con net.PacketConn
	db *mgo.DB
	session *mongo.Session
)

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
	
	session, err = mgo.Mongo(fmt.Sprintf("%s:%s",st.DbServer, st.DbPort))
	if err != nil{
		fmt.Println("DB connection error.",err); 
		return;
	}
	
	// switch to monotonic mode
	session.SetMode(mgo.Monotonic, true)

	db = session.DB(st.DbName)
	ColStream = db.C("ndayak_streams")
	ColPost = db.C("user_post")
	ColChan = db.C("channel")
	ColTun = db.C("tunnel")
	ColUser = db.C("user")
	ColUserSettings = db.C("user_settings")
	
	index = mgo.Index{
	    Key: []string{"userid", "postid"},
	    Unique: true,
	    DropDups: true,
	    Background: true,
	    Sparse: true,
	}
	
	err = ColStream.EnsureIndex(index)
	if err != nil {
		fmt.Println("Cannot ensure index for `userid` and `postid`")
	}
	
	/*
	atreps = map[string]string{
		"_origin_id":"origin_id_",
		"_metaname_":"metaname_",
		"_followed_user_ids":"followed_user_ids_",
		"_writer_id":"writerid",
		"_popular_post":"popular_post_",
		"_user_id":"user_id_",
	}
	*/
}

func ProcessPost(post_id string){

	if session == nil { Error("Database not connected.\n"); return;}

	// get post
	
	/*
	qfind, err := mongo.Marshal(oidSearch{"_id":mongo.ObjectId{post_id}}, atreps)
	if err != nil{Error("Cannot marshal. %s\n", err); return;}

	doc, err := ColPost.FindOne(qfind)
	if err != nil{
		Warn("Cannot find post by id `%s`. %s.\n",post_id,err)
		return
	}
	
	
	var post UserPost
	
	mongo.Unmarshal(doc.Bytes(), &post, atreps)
	*/
	
	var dbrv map[string]interface{}
	
	ColPost.Find(bson.M{"_id":post_id}).One(&dbrv)
	
	
	Info2("Got post id: %v, writer: %v, origin: %v\n",dbrv['_id'], db['_writer_id'], db['_origin_id'])
	
	// get writer
	
	/*
	writer, err := GetUser(post.WriterId)
	if err != nil{
		Warn("Cannot get writer with id `%s` for post id `%s`. err: %v\n", post.WriterId, strid(post.Id_), err)
		return
	}
	*/
	
	
	
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
			
			if strid(follower.Id_) == post.Origin_id_{
				continue
			}
			
			Info2("broadcast to follower: id: %v, name: %v\n", strid(follower.Id_), follower.Name)
		
			// insert to follower streams
			InsertPostStream(strid(follower.Id_), post_id)
		}
	}else{
		Warn("Cannot find post by id `%s`. %s.\n",post_id,err)
	}
	
	
	// get origin
	if len(strings.Trim(post.Origin_id_," \n\t\r")) == 0{
		Warn("post with id `%s` has no origin id\n", strid(post.Id_))
		return
	}
	
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
		case "User":
			Info2("Inserting into origin=User. origin_id: %v\n", post.Origin_id_)
			InsertPostStream(post.Origin_id_, post_id)
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
		
			var user User
			mongo.Unmarshal(doc.Bytes(), &user, atreps)
			user_id := strid(user.Id_)
			Info2("broadcast to all: id: %v, name: %v\n", user_id, user.Name)
			
			// get user settings, is accept feed pop article
			st, err := GetUserSettings(user_id)
			if err != nil{
				Error("BroadcastAll: Cannot get user settings for user id `%v`\n", user_id)
				continue
			}
			if st.Feed_pop_article == false{
				Info2("BroadcastAll: Ignore feed article for user id `%v`\n", user_id)
				continue
			}
		
			// insert to follower streams
			InsertPostStream(user_id, postId)
		}
	}else{
		Warn("Cannot find post by id `%s`. %s.\n",postId,err)
	}
	
}
