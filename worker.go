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
	"sync"
	"container/vector"
	"./_obj/ndayak"
)

const (
	CMD_QUIT = "/q"
	CMD_PROCESS = "/p" // /p [POST-ID]
	CMD_RMPOST = "/rm" // /rm [POST-ID]
	CMD_UPPOST = "/up" // /up [POST-ID]
	CMD_BPTALL = "/bptall" // /bptall [POST-ID]
	CMD_HI = "/hi" // /hi there
	CMD_SRV = "/srv"		// /srv [NDAYAK-IP] [down|up]
)

var (
	con net.PacketConn
	asLoadBalancer bool
	loadbalServers vector.StringVector
	loadbalServersCount int
	loadbalConns vector.Vector
	redirectorMutex *sync.Mutex
)

func Init(_con net.PacketConn, asLoadbal bool, loadbalsrv string){
	con = _con
	asLoadBalancer = asLoadbal
	
	// if in load balancher mode then
	// create exporter and importer
	if asLoadbal == true{
		redirectorMutex = new(sync.Mutex)
		servers := strings.Split(loadbalsrv,",",1000)
		for _, server := range servers{
			if len(strings.Trim(server," \n\t\r")) > 0{
				loadbalServers.Push(server)
			}
		}
		loadbalServersCount = loadbalServers.Len()
		for i := 0; i < loadbalServersCount; i++ {
			conn, err := net.Dial("udp4","",loadbalServers[i])
			if err != nil{
				ndayak.Error("worker.Init: Cannot dial to udp4 for load bal server: `%s`\n", loadbalServers[i])
			}
			ndayak.Info2("Send hi to `%v`\n", loadbalServers[i])
			conn.Write([]byte("/hi"))
			loadbalConns.Push(conn)
		}
	}
	
}

func Close(){
	ndayak.Info2("Closing...\n")
	if asLoadBalancer == true{
		for i := 0; i < loadbalServersCount; i++ {
			ndayak.Info2("Closing server `%s`...\n", loadbalServers[i])
			loadbalConns[i].(net.Conn).Close()
		}
	}
}

func Worker(ch chan string){
	for {
		rv := <- ch
		//fmt.Println("worker received new task...")
		//fmt.Println("working...")
		
		d := strings.Split(rv, " ", 2)
		
		var arg string = ""
		
		cmdstr := d[0]
		
		if cmdstr[0] != '/'{
			ndayak.Warn("Invalid command\n")
			continue
		}
		
		if asLoadBalancer == true && cmdstr != CMD_SRV{
			redirectCmd(rv)
			continue
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
			ndayak.Info("Quit command received. Realy quiting now...\n")
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
			
		case CMD_BPTALL:
			post_id := arg
			ndayak.Info("Broadcast all post with id `%s`\n", post_id)
			ndayak.BroadcastAll(post_id)
			
		case CMD_SRV:
			
			if asLoadBalancer == false{
				ndayak.Warn("%s command not supported for non load balancer mode\n", cmdstr)
				continue
			}
			
			d := strings.Fields(arg)

			if len(d) < 2{ ndayak.Warn("Invalid `%s` command\n", cmdstr); continue; }
			addr := d[0]
			state := d[1]
			if len(strings.Trim(addr," \n\t\r")) == 0{
				ndayak.Warn("Invalid ndayak ip")
				continue
			}
			if state == "up"{
				addNewNdayak(addr)
				ndayak.Info("Ndayak instance for addr `%s` has been added.\n", addr)
			}else{
				rmNdayak(addr)
				ndayak.Info("Ndayak instance for addr `%s` already removed.\n", addr)
			}
			
		case CMD_HI:
			if asLoadBalancer == true{
				redirectCmd(rv)
			}else{
				ndayak.Info("Got \"hi\" message from Ndayaks\n")
			}
			
		default:
			ndayak.Info("Unknown command `%s`\n", cmdstr)
		}
		//fmt.Println("worker finished task.")
	}
}

var currentChExIndex = 0;

func ndayakExists(addr string) bool{
	for _, s := range loadbalServers{
		if s == addr{
			return true
		}
	}
	return false
}

func addNewNdayak(addr string){
	redirectorMutex.Lock()
	defer redirectorMutex.Unlock()
	
	if ndayakExists(addr) == true{
		ndayak.Warn("Ndayak with addr `%s` already exists\n", addr)
		return
	}
	conn, err := net.Dial("udp4","",addr)
	if err != nil{
		ndayak.Error("addNewNdayak: Cannot connect to new ndayak addr `%v`\n", addr)
		return
	}
	loadbalServers.Push(addr)
	loadbalConns.Push(conn)
}

func rmNdayak(addr string){
	redirectorMutex.Lock()
	defer redirectorMutex.Unlock()
	
	if ndayakExists(addr) == false{
		ndayak.Warn("Ndayak with addr `%s` doesn't exists\n", addr)
		return
	}
	for i, s := range loadbalServers{
		if s == addr{
			loadbalServers.Delete(i)
			loadbalConns.At(i).(net.Conn).Close()
			loadbalConns.Delete(i)
			break
		}
	}
}

func redirectCmd(cmd string){
	redirectorMutex.Lock()
	defer redirectorMutex.Unlock()
	
	if loadbalServers.Len() == 0 || loadbalConns.Len() == 0{
		ndayak.Warn("redirectCmd: No route to any ndayak instance. Please add first.\n")
		return
	}
	
	ndayak.Info("Redirect command `%s` to `%v`\n", cmd, loadbalServers)
	
	if currentChExIndex >= loadbalServers.Len(){
		currentChExIndex = 0;
	}
	n, err := loadbalConns.At(currentChExIndex).(net.Conn).Write([]byte(cmd))
	if err != nil{
		ndayak.Error("redirectCmd: Cannot send data to ladbal server `%s`. %s\n", loadbalServers.At(currentChExIndex), err)
	}
	ndayak.Info2("Success send to loadbal server `%s`, packet for %d bytes\n", loadbalServers.At(currentChExIndex), n)
	currentChExIndex += 1;
}




