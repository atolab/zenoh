source ../common/init.sh

graphname=$(basename $1)

folder=run_${graphname}_`date +"%y-%m-%d_%H-%M"`

mkdir $folder

cp $1 $folder/$graphname

neato -Tpng $1 -o $folder/$graphname.png

run_brokers $1 $folder

sleep 2

gengraph $1 $folder

cleanall

convert $folder/$graphname.png $folder/$graphname--trees.png -append $folder/$graphname-all.png

