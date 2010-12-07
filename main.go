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

package main

import (
	"fmt"
	"net"
	"os"
	"./core"
)

var (
	laddr *net.UDPAddr
	con *net.UDPConn
	err os.Error
)


func main(){
	
	var buf[1000] byte;

	laddr, err = net.ResolveUDPAddr("127.0.0.1:6854");
	if err != nil{fmt.Println("Error in resolve... ",err); os.Exit(1);}
	
	con, err = net.ListenUDP("udp", laddr)
	if err != nil{fmt.Println("Error in listen..."); os.Exit(2);}

	resp := make(chan int)
	
	core.Init(con)

	go core.Worker(resp)
	go core.Worker(resp)
	go core.Worker(resp)

	for{
		n, err := con.Read(buf[0:128]);
		if err != nil{fmt.Println("Error in read..."); os.Exit(3);}
		fmt.Println("Read",n,"bytes")
		
		go func(ch chan int){
			var d string = string(buf[:n]);
			fmt.Println("Got:",d)
			switch d{
			case "q":
				ch <- 101
			}
		}(resp)
	}
	
	fmt.Println("Done.")
	
}

