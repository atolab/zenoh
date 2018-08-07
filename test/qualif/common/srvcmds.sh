
for i in `cat $1 | grep "\-\-" | sed "s% *\([0-9]*\) *-- *\([0-9]*\)[^0-9].*%\1\n\2%g" | sort -r -u`
do
  peers=""
  for j in `cat $1 | grep -e "${i} *\-\-" | sed "s% *\([0-9]*\) *-- *\([0-9]*\)[^0-9].*%\2%g"`
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
    echo "runproc zenohd_$i zenohd.exe -t $i"
  else
    echo "runproc zenohd_$i zenohd.exe -t $i -p $peers"
  fi
  
done
