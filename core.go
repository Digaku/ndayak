/*
 * Copyright (c) 2010 Digaku.com
 * writen by Robin Syihab (r[at]nosql.asia)
 *
 * License MIT
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
	"mongo"
)

var (
	con *net.UDPConn
	col *mongo.Collection
)

const (
	CMD_QUIT = "/q"
	CMD_PROCESS = "/p" // /p [POST-ID];[WRITER-ID]
)

type Settings struct {
	DbServer string
	DbPort int
}

var settings *Settings;

func Init(_con *net.UDPConn, st *Settings){
	con = _con
	settings = st
	
	dbcon, err := mongo.Connect(st.DbServer,st.DbPort)
	if err != nil{fmt.Println("DB connection error.",err); return;}
	
	col = dbcon.GetDB("digaku").GetCollection("ndayak_streams")
	
}

func Worker(ch chan string){
	for {
		rv := <- ch
		//fmt.Println("worker received new task...")
		//fmt.Println("working...")
		
		switch rv[:2]{
		case CMD_PROCESS:

			d := strings.Split(strings.Split(rv," ",2)[1],";",2)
			
			post_id := d[0]
			writer_id := d[1]
			
			fmt.Printf("Building index for post_id: %v, writer_id: %v...\n", post_id, writer_id)
			
			//doc, _ := mongo.Marshal(map[string]string{
			//	"post_id": post_id
			//})
			
			//col.Insert(doc)
			
		case CMD_QUIT:
			fmt.Println("Quit command received. Realy quiting now...")
			con.Close()
			os.Exit(0)
		}
		//fmt.Println("worker finished task.")
	}
}

