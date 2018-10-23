

run_brokers()
{
  graph=$1
  graphname=$(basename $1)
  defaultfolder=run_${graphname}_`date +"%y-%m-%d_%H-%M"`
  folder=${2:-$defaultfolder}
  delay=${3:-0}

  mkdir $folder
  
  for i in `getnodes $graph | sort -r -u`
  do
    runproc zenohd-$i $folder `broker_cmd $graph $i`
    sleep $delay
  done
}


run_client()
{
  runproc zenohc-$1 $2 zenohc.exe -p tcp/127.0.0.1:$1
}

sub()
{
  printf "open\ndres $1 $2\ndsub 1\n" 
}

pub()
{
  printf "open\nwriten $1 $2 1 $3\ndsub $1\n" 
}

monitorflow()
{
  genflowgraph $1 $2 live

  "${3:-code}" -n /Users/olivier/workspaces/zenoh/test/tree/$2/$(basename $1)-live-flow.png

  while true
  do 
    sleep ${4:-0}
    genflowgraph $1 $2 live
  done
}