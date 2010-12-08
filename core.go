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
	"strings"
	"encoding/hex"
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
	CMD_RMPOST = "/rm" // /rm [POST-ID]
)

var atreps map[string]string

type Settings struct {
	DbServer string
	DbPort int
	DbName string
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
	WriterId string
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
	
	db = dbcon.GetDB(st.DbName)
	colStream = db.GetCollection("ndayak_streams")
	colPost = db.GetCollection("user_post")
	colChan = db.GetCollection("channel")
	colTun = db.GetCollection("tunnel")
	colUser = db.GetCollection("user")
	
	colStream.EnsureIndex("ndayax_1",map[string]int{"userid":1,"postid":1})
	
	atreps = map[string]string{
		"_origin_id":"origin_id_",
		"_metaname_":"metaname_",
		"_followed_user_ids":"followed_user_ids_",
		"_writer_id":"writerid",
	}
}

func Worker(ch chan string){
	for {
		rv := <- ch
		//fmt.Println("worker received new task...")
		//fmt.Println("working...")
		
		d := strings.Fields(rv)
		
		var arg string = ""
		
		cmdstr := d[0]
		
		if len(d) > 1{
			arg = d[1]
		}
		
		switch cmdstr{
		case CMD_PROCESS:
		 	post_id := arg
			
			fmt.Printf("Building index for post_id: %v...\n", post_id)
			process_post(post_id)
			
		case CMD_QUIT:
			fmt.Println("Quit command received. Realy quiting now...")
			con.Close()
			os.Exit(0)
			
		case CMD_RMPOST:
			post_id := arg
			fmt.Printf("Removing stream for post id: `%s`...\n", post_id)
			rmPostStream(post_id)
		default:
			fmt.Printf("Unknown command `%s`\n", cmdstr)
		}
		//fmt.Println("worker finished task.")
	}
}

type oidSearch map[string]mongo.ObjectId

func rmPostStream(postId string){
	qfind, err := mongo.Marshal(map[string]string{"postid":postId}, atreps)
	if err != nil{err = os.NewError("Cannot marshal"); return}
	
	err = colStream.Remove(qfind)
	if err != nil{
		fmt.Printf("rmPost: Cannot remove post with id `%s`\n", postId)
	}
}

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

func getUser(userId string) (user *User, err os.Error){
	qfind, err := mongo.Marshal(oidSearch{"_id":mongo.ObjectId{userId}}, atreps)
	if err != nil{
		return nil, os.NewError(fmt.Sprintf("getUser: Cannot marshal. %s", err))
	}

	doc, err := colUser.FindOne(qfind)
	if err != nil{
		return nil, os.NewError(fmt.Sprintf("getUser: Cannot find user by id `%s`. %s.",userId,err))
	}
	
	user = new(User)
	
	mongo.Unmarshal(doc.Bytes(), user, atreps)
	
	return user, nil
}

func postStreamExists(userId string, postId string) bool {
	
	if len(userId) == 24 && len(postId) == 24{
		qfind, err := mongo.Marshal(&PostStream{UserId:userId,PostId:postId}, atreps)
		if err != nil{fmt.Printf("Cannot marshal. %s\n", err); return false}
		doc, err := colStream.FindOne(qfind)
		if err == nil || doc != nil{
			return true
		}
	}

	return false
}

func insertPostStream(userId string, postId string) {
	if len(userId) == 0 || len(postId) == 0{
		return
	}
	if postStreamExists(userId, postId){
		fmt.Printf("Cannot insertPostStream for userId: %v, postId: %v. Already exists.\n", userId, postId)
		return
	}
	doc, err := mongo.Marshal(map[string]string{"_metaname_":"NdayakStream","userid":userId,"postid":postId}, atreps)
	if err != nil{
		fmt.Printf("Cannot insertPostStream for userId: %v, postId: %v\n", userId, postId)
		return
	}
	colStream.Insert(doc)
}

func strid(byteId []byte) string {return hex.EncodeToString(byteId);}
func byteid(strId string) []byte{
	rv, err := hex.DecodeString(strId)
	if err != nil{
		return nil
	}
	return rv
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
	
	fmt.Printf("Got post id: %v, writer: %s, origin: %s\n",strid(post.Id_), post.WriterId, post.Origin_id_)
	
	// get writer
	writer, err := getUser(post.WriterId)
	if err != nil{
		fmt.Printf("Cannot get writer with id `%s` for post id `%s`. err: %v\n", post.WriterId, strid(post.Id_), err)
		return
	}
	
	fmt.Printf("writer: %v\n", writer.Name)
	insertPostStream(strid(writer.Id_), post_id)
	
	var writer_id string = strid(writer.Id_)
	fmt.Printf("writer_id: %s\n", writer_id)
	
	// get all followers
	qfind, err = mongo.Marshal(map[string]string{"_followed_user_ids":writer_id}, atreps)
	if err != nil{fmt.Printf("Cannot marshal. %s\n", err); return;}

	// broadcast to all followers
	cursor, err := colUser.FindAll(qfind)
	if err == nil{
		for cursor.HasMore(){
			doc, err = cursor.GetNext()
			if err != nil{fmt.Printf("Cannot get next. e: %v\n", err); break}
		
			var follower User
			mongo.Unmarshal(doc.Bytes(), &follower, atreps)
			fmt.Printf("follower: id: %v, name: %v\n", strid(follower.Id_), follower.Name)
		
			// insert to follower streams
			insertPostStream(strid(follower.Id_), post_id)
		}
	}else{
		fmt.Printf("Cannot find post by id `%s`. %s.\n",post_id,err)
	}
	
	
	// get origin
	doc, err = getOrigin(post.Origin_id_)
	if err == nil{

		var spdoc SuperDoc

		mongo.Unmarshal(doc.Bytes(), &spdoc, atreps)

		switch spdoc.Metaname_{
		case "Channel":
			var ch Channel
			mongo.Unmarshal(doc.Bytes(), &ch, atreps)

			var ch_id string = strid(ch.Id_)
			fmt.Printf("ch_id: %s\n", ch_id)

			// get all members
			qfind, err := mongo.Marshal(map[string]string{"_channel_ids":strid(ch.Id_)}, atreps)
			if err != nil{fmt.Printf("Cannot marshal. %s\n", err); return;}
			
			// broadcast to all members
			cursor, err := colUser.FindAll(qfind)
			if err == nil{
				for cursor.HasMore(){
					doc, err = cursor.GetNext()
					if err != nil{fmt.Printf("Cannot get next. e: %v\n", err); break}

					var follower User
					mongo.Unmarshal(doc.Bytes(), &follower, atreps)
					fmt.Printf("member: id: %v, name: %v\n", strid(follower.Id_), follower.Name)

					// insert to follower streams
					insertPostStream(strid(follower.Id_), post_id)
				}
			}else{
				fmt.Printf("Cannot find post by id `%s`. %s.\n",post_id,err)
			}
		}

	}else{
		fmt.Printf("Cannot get origin id `%s`\n", post.Origin_id_)
	}
	
}

