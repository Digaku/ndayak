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


package worker

import (
	//"fmt"
	"strings"
	"os"
	"net"
	"./_obj/ndayak"
)

const (
	CMD_QUIT = "/q"
	CMD_PROCESS = "/p" // /p [POST-ID]
	CMD_RMPOST = "/rm" // /rm [POST-ID]
	CMD_UPPOST = "/up" // /up [POST-ID]
)

var (
	con *net.UDPConn
)

func Init(_con *net.UDPConn){
	con = _con
}

func Worker(ch chan string){
	for {
		rv := <- ch
		//fmt.Println("worker received new task...")
		//fmt.Println("working...")
		
		d := strings.Fields(rv)
		
		var arg string = ""
		
		cmdstr := d[0]
		
		if cmdstr[0] != '/'{
			return
		}
		
		if len(d) > 1{
			arg = d[1]
		}
		
		switch cmdstr{
		case CMD_PROCESS:
		 	post_id := arg
			
			ndayak.Info("Building index for post_id: %v...\n", post_id)
			ndayak.ProcessPost(post_id)
			
		case CMD_QUIT:
			ndayak.Info("Quit command received. Realy quiting now...")
			con.Close()
			os.Exit(0)
			
		case CMD_RMPOST:
			post_id := arg
			ndayak.Info("Removing stream for post id: `%s`...\n", post_id)
			ndayak.RmPostStream(post_id)
			
		case CMD_UPPOST:
			post_id := arg
			ndayak.Info("Top up stream for post id: `%s`...\n", post_id)
			ndayak.TopUpPost(post_id)
			
		default:
			ndayak.Info("Unknown command `%s`\n", cmdstr)
		}
		//fmt.Println("worker finished task.")
	}
}

