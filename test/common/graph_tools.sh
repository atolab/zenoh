
get_nodeid()
{
  echo $1 | cut -d' ' -f 4 | cut -d')' -f1
}

get_parent()
{
  echo $1 | cut -d'(' -f 9 | cut -d')' -f1
}

getnodes()
{
  cat $1 | grep "\-\-" | sed "s% *\([0-9]*\) *-- *\([0-9]*\)[^0-9].*%\1|\2%g" | tr '|' '\n' | sort -u
  #cat $1 | grep "\-\-" | sed "s%--%%g" | sed "s%{%%g" | sed "s%}%%g" | sed "s%\([^;]*\);.*%\1%g" | tr -s ' ' '\n' | sed '/^\s*$/d' | sort -u 
}


broker_cmd()
{
    peers=""
    for j in `cat $1 | grep -e "$2 *\-\-" | sed "s% *\([0-9]*\) *-- *\([0-9]*\)[^0-9].*%\2%g"`
    do 
    if [[ $peers == "" ]] 
      then 
        peers="tcp/127.0.0.1:$j"
      else
        peers="$peers,tcp/127.0.0.1:$j"
      fi
    done
    #printf "%-100s > %s\n" "zenohd.exe -t $i -p $peers" "\$outdir/zenohd_$i.log 2>&1"
    if [[ $peers == "" ]] 
    then 
      echo "zenohd.exe -t $2"
    else
      echo "zenohd.exe -t $2 -p $peers"
    fi
}

run_brokers()
{
  defaultfolder=run_$1_`date +"%y-%m-%d_%H-%M"`
  for i in `getnodes $1 | sort -r -u`
  do
    runproc zenohd-$i ${2:-$defaultfolder} `broker_cmd $1 $i`
    eval "BKR_${i}=$?"
    sleep ${2:-0}
  done
}

remap_ids()
{
  for i in `ls $1/*.log`;
  do
    cp $i $i.map
  done

  for i in `ls $1/*.log`;
  do
    PORT=`basename $i | cut -d '-' -f2 | cut -d '.' -f1`;
    IDS=`cat $i | grep "   Local : " | sed "s%.*node_id \([^)]*\)).*%\1%g" | sort -u`
    #echo "$i : $IDS => $PORT";
  
    for j in `ls $1/*.log`;
    do 
      for ID in $IDS;
      do
        sed -e "s%$ID%$PORT%g" -i "" $j.map;
      done
    done
  done
}

gengraph()
{
  graph=$1
  graphname=$(basename $graph)
  folder=$2
  suffix=$3
  output=$folder/$graphname-$suffix-trees

  remap_ids $folder

  colors[0]="red"
  colors[1]="blue"
  colors[2]="green"
  colors[3]="magenta"
  colors[4]="cyan"
  colors[5]="yellow"


  echo "digraph G {" > $output

  # copy all nodes
  cat $graph | grep -v "{" | grep -v "}" | grep -v "\-\-" >> $output

  for i in $(getnodes $graph)
  do
    status=`cat $folder/zenohd-$i.log.map | grep "Local\|kill" | tail -n 1`
    if [[ $status == *"kill"* ]]
    then 
      echo "  $i [style=dotted]" >> $output
    else 
      for j in 0 1 2 3 4 5
      do 
        status=`cat $folder/zenohd-$i.log.map | grep "Local" | grep "tree_nb $j" | tail -n 1`
        if [[ ! $status == "" ]]
        then 
          nodeid=$(get_nodeid "$status")
          parent=$(get_parent "$status")
          
          if [[ $parent == "" ]]
          then 
            echo "  $nodeid [fillcolor=${colors[$j]}, style=filled]" >> $output
          fi
        fi
      done
    fi
  done

  echo "  subgraph Base {" >> $output
  echo "    edge [dir=none; style=dashed]" >> $output
  cat $graph | grep "\-\-" | sed "s%--%->%g" >> $output
  echo "  }" >> $output

  for j in 0 1 2 3 4 5
  do 
    
    echo "  subgraph Tree$j {" >> $output
    echo "      edge [color=${colors[$j]}]" >> $output
    for i in $(getnodes $graph)
    do
      cat $folder/zenohd-$i.log.map | grep "Local\|kill" | grep "tree_nb $j\|kill" | tail -n 1 | while read status
      do 
        if [[ ! $status == *"kill"* ]]
        then 
          nodeid=$(get_nodeid "$status")
          parent=$(get_parent "$status")
          if [[ $parent != "" ]]
          then 
            echo "      $nodeid -> $parent" >> $output
          fi
        fi
      done 
    done
    echo "  }" >> $output
      
  done
  echo "}" >> $output

  neato -Tpng $output -o $folder/$graphname-$suffix-trees.png
  echo "generated $folder/$graphname-$suffix-trees.png"
}

monitor()
{
  gengraph $1 $2 live

  "${3:-code}" -n /Users/olivier/workspaces/zenoh/test/tree/tmp/$(basename $1)-live-trees.png

  while true
  do 
    sleep ${4:-0}
    gengraph $1 tmp live
  done
}
