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
	"os"
	"net"
	"time"
	"encoding/hex"
	"mongo"
)

var (
	Con *net.UDPConn
	Db *mongo.Database
	ColStream *mongo.Collection
	ColPost *mongo.Collection
	ColChan *mongo.Collection
	ColTun *mongo.Collection
	ColUser *mongo.Collection
)


type OidSearch map[string]mongo.ObjectId

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
	Metaname_ string
	UserId string
	PostId string
	Timepos int64
}

type searchPost struct {
	Id_ []byte
}


func RmPostStream(postId string){
	qfind, err := mongo.Marshal(map[string]string{"postid":postId}, atreps)
	if err != nil{err = os.NewError("Cannot marshal"); return}
	
	err = ColStream.Remove(qfind)
	if err != nil{
		Error("rmPost: Cannot remove post with id `%s`\n", postId)
	}
}

func GetOrigin(originId string) (doc mongo.BSON, err os.Error){
	
	qfind, err := mongo.Marshal(OidSearch{"_id":mongo.ObjectId{originId}}, atreps)
	if err != nil{err = os.NewError("Cannot marshal"); return}
	
	doc, err = ColUser.FindOne(qfind)
	if err != nil{
		doc, err = ColChan.FindOne(qfind)
		if err != nil{
			doc, err = ColTun.FindOne(qfind)
			if err != nil{
				err = os.NewError(fmt.Sprintf("Cannot find origin for id `%s`", originId))
				return
			}
		}
	}

	return doc, nil
}

func GetUser(userId string) (user *User, err os.Error){
	qfind, err := mongo.Marshal(OidSearch{"_id":mongo.ObjectId{userId}}, atreps)
	if err != nil{
		return nil, os.NewError(fmt.Sprintf("getUser: Cannot marshal. %s", err))
	}

	doc, err := ColUser.FindOne(qfind)
	if err != nil{
		return nil, os.NewError(fmt.Sprintf("getUser: Cannot find user by id `%s`. %s.",userId,err))
	}
	
	user = new(User)
	
	mongo.Unmarshal(doc.Bytes(), user, atreps)
	
	return user, nil
}

func PostStreamExists(userId string, postId string) bool {
	
	if len(userId) == 24 && len(postId) == 24{
		qfind, err := mongo.Marshal(map[string]string{"userid":userId,"postid":postId}, atreps)
		if err != nil{Error("Cannot marshal. %s\n", err); return false}
		doc, err := ColStream.FindOne(qfind)
		if err == nil || doc != nil{
			return true
		}
	}

	return false
}

func InsertPostStream(userId string, postId string) {
	
	Info2("Inserting Post stream with id `%v`, writer id: `%v`\n", postId, userId)
	
	if len(userId) == 0 || len(postId) == 0{
		return
	}
	if PostStreamExists(userId, postId){
		Warn("InsertPostStream: Cannot insert for userId: %v, postId: %v. Already exists.\n", userId, postId)
		return
	}

	doc, err := mongo.Marshal(&PostStream{
		Metaname_:"NdayakStream",
		UserId:userId,
		PostId:postId,
		Timepos:time.Nanoseconds() / 1e6,
		}, atreps)
	if err != nil{
		Error("Cannot insertPostStream for userId: %v, postId: %v. %v\n", userId, postId, err)
		return
	}
	ColStream.Insert(doc)
}

func strid(byteId []byte) string {return hex.EncodeToString(byteId);}
func byteid(strId string) []byte{
	rv, err := hex.DecodeString(strId)
	if err != nil{
		return nil
	}
	return rv
}

