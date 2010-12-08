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

package main

import (
	"fmt"
	"net"
	"os"
	"flag"
	"./core"
)

var (
	VERSION string = "v0.1 alpha"
	laddr *net.UDPAddr
	con *net.UDPConn
	err os.Error
)


func banner(){
	fmt.Printf("Ndayak %s\n",VERSION)
}


var listen_port = flag.Int("port",50105,"Listen port")
var db_server = flag.String("dbserver","127.0.0.1","Database/collection server")
var db_port = flag.Int("dbport",27017,"Database/collection port")
var db_name = flag.String("dbname","test","Database name")

func main(){

	flag.Parse()
	
	banner()
	
	fmt.Printf("options:\n\tdb_server: %s:%d\n\tdb_name: %s\n", *db_server, *db_port, *db_name)

	var listen_addr string = fmt.Sprintf("0.0.0.0:%d",*listen_port)
	
	laddr, err = net.ResolveUDPAddr(listen_addr);
	if err != nil{fmt.Println("Error in resolve... ",err); os.Exit(1);}
	
	con, err = net.ListenUDP("udp", laddr)
	if err != nil{fmt.Println("Error in listen..."); os.Exit(2);}
	
	fmt.Println("Listening at " + listen_addr + "...")
	fmt.Println("Ready for connection.")

	resp := make(chan string)

	st := core.Settings{*db_server,*db_port,*db_name}
	
	core.Init(con, &st)

	go core.Worker(resp)
	go core.Worker(resp)
	go core.Worker(resp)
	
	go stream_reader(resp,1)
	go stream_reader(resp,2)
	stream_reader(resp,3) // main stream-reader

	
	fmt.Println("Done.")
	
}

func stream_reader(resp chan string,id int){
	var buf[1000] byte;
	for{
		n, err := con.Read(buf[0:128]);
		if err != nil{fmt.Println("Error in read..."); os.Exit(3);}
		
		fmt.Println(fmt.Sprintf("[sr-%d]",id),"received",n,"bytes")
	
		go func(ch chan string){
			var d string = string(buf[:n]);
			fmt.Println("Got:",d)
			ch <- d
		}(resp)
	}	
}


