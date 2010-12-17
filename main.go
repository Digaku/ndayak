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
	"strings"
	"flag"
	"./_obj/ndayak"
	"./worker"
	sp "./simpleconfig"
)

var (
	VERSION string = "v0.4 alpha"
	laddr *net.UDPAddr
	con net.PacketConn
	err os.Error
)


func banner(){
	fmt.Printf("Ndayak %s\n",VERSION)
}

type Config struct{
	ListenPort int
	DbServer string
	DbPort int
	DbName string
	Verbosity int
	AsLoadBalancer bool
	ConfigFile string
	Servers string
}


var listenPort = flag.Int("port",50105,"Listen port")
var DbServer = flag.String("dbserver","127.0.0.1","Database/collection server")
var DbPort = flag.Int("dbport",27017,"Database/collection port")
var DbName = flag.String("dbname","test","Database name")
var verbosity = flag.Int("v",0,"Verbosity level")
var asLoadBalancer = flag.Bool("as-load-balancer",false,"Run Ndayak as load balancer")
var configFile = flag.String("config","ndayak.conf","Configuration file")

func main(){

	flag.Parse()
	
	banner()
	
	f, err := os.Open(*configFile,os.O_RDONLY,0666)
	if err != nil{
		fmt.Printf("Cannot open configuration file `%s`\n", *configFile)
		os.Exit(1)
		return
	}
	defer f.Close()
	
	var config Config
	err = sp.Unmarshal(&config, f)
	if err != nil{
		fmt.Printf("Cannot parse configuraition from file `%s`\n", *configFile)
		os.Exit(1)
		return
	}
	
	if *listenPort != 50105{
		config.ListenPort = *listenPort
	}

	if *verbosity != 0{
		config.Verbosity = *verbosity
	}
	
	fmt.Printf("config.AsLoadBalancer: %v\n", config.AsLoadBalancer)
	if config.AsLoadBalancer == true{
		*asLoadBalancer = true
	}
	
	fmt.Printf("options:\n\tdb_server: %s:%d\n\tdb_name: %s\n", config.DbServer, config.DbPort, config.DbName)
	fmt.Printf("\tverbosity: %d\n", config.Verbosity)

	var listen_addr string = fmt.Sprintf("0.0.0.0:%d",config.ListenPort)
	
	//laddr, err = net.ResolveUDPAddr(listen_addr);
	//if err != nil{fmt.Println("Error in resolve... ",err); os.Exit(1);}
	
	con, err = net.ListenPacket("udp4", listen_addr)
	if err != nil{fmt.Println("Error in listen..."); os.Exit(2);}
	
	if *asLoadBalancer == true{
		fmt.Println("Run as load balancer")
		
		// split servers
		servers := strings.Split(config.Servers, ",", 10)
		for _, s := range servers{
			if len(strings.Trim(s," \n\t\r")) == 0{
				fmt.Println("[WARNING] No servers found for load balancer")
				servers = []string{}
				break
			}
		}
		fmt.Printf("Servers (%d): %v\n", len(servers),servers)
	}
	fmt.Println("Listening at " + listen_addr + "...")
	fmt.Println("Ready for connection.")

	resp := make(chan string)

	st := ndayak.Settings{config.DbServer,config.DbPort,config.DbName}

	ndayak.Init(con, &st, config.Verbosity)
	worker.Init(con, *asLoadBalancer, config.Servers)

	go worker.Worker(resp)
	go worker.Worker(resp)
	go worker.Worker(resp)
	
	go stream_reader(resp,1)
	go stream_reader(resp,2)
	stream_reader(resp,3) // main stream-reader

	worker.Close()
	
	fmt.Println("Done.")
	
}

func stream_reader(resp chan string,id int){
	var buf[1000] byte;
	for{
		n, _, err := con.ReadFrom(buf[0:128]);
		if err != nil{fmt.Println("Error in read..."); os.Exit(3);}
		
		ndayak.Info2("[sr-%d] received %d bytes\n", id, n)
	
		go func(ch chan string){
			var d string = string(buf[:n]);
			ndayak.Info2("Got: %s\n",d)
			ch <- d
		}(resp)
	}	
}


