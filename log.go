

package ndayak

import (
	"fmt"
	"os"
)

var Quiet bool
var verbosity int = 0

func SetVerbosity(level int){
	verbosity = level
}

func Info2(format string, v ...interface{}) {
	if verbosity < 3 {return;}
	fmt.Printf("[info-2] " + format, v...);	
}

func Info(format string, v ...interface{}) {
	if verbosity < 2 {return;}
	fmt.Printf("[info] " + format, v...);
}
func Warn(format string, v ...interface{}) {
	if verbosity < 1 {return;}
	fmt.Printf("[warning] " + format, v...);
}
func Error(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr,"[error] " + format, v...);
}




