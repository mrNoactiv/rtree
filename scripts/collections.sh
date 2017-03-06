#!/bin/bash 
prefix=CPU
loop=5;
exec=../rtree_gpu
wd=./tmp
dropbox=/home/fei/bed157/Dropbox/Results/Rtree/20140623/

#rm *.csv
#rm -rf $wd
mkdir $wd

function test
{
	exec=../rtree_cpu
	prefix=CPU
	#run $1 $2 $3
	exec=../rtree_gpu
	prefix=GPU
	run $1 $2 $3
}

function run
{

for tpb in 128 256 384 512 768 1024
do
  	for ch in 1 16 32 64 128 256
	do		
		for bs in 2048 4096 8192 16384
		do
			resultFile="$dropbox$prefix"_$1_$bs_tpb_$tpb_chunks_$ch.csv
			info $1 $2 $3 $bs 1 $loop $tpb $ch $curentTest $
			if [ -f $wd/quickdb_$1_$bs.dat ]; then
				create=0
			else
				create=1
			fi
			$exec --create-db $create --block-size $bs -r $resultFile -c $2 -q $3 -db $wd/quickdb_$1_$bs --gpu-chunks $ch --gpu-tpb $tpb 2>error.txt
			for ((i=2; i<=($loop); i++))
			do
				info $1 $2 $3 $bs $i $loop $tpb $ch
				$exec --create-db 0 --block-size $bs -r $resultFile -c $2 -q $3 -db $wd/quickdb_$1_$bs --gpu-chunks $ch --gpu-tpb $tpb 2>error.txt
			done
		
		done
	done
done
}

function info
{
	clear
	echo -e "\e[34m Name: $1"
	echo -e "\e[34m Collection: $2"
	echo -e "\e[34m Queries: $3"
	echo -e "\e[34m Device: $prefix"
	echo -e "\e[34m Block size: $4"
	echo -e "\e[34m GPU TPB: $7"
	echo -e "\e[34m GPU Chunks: $8"
	echo -e "\e[34m Test \e[36m$5 \e[34mof \e[36m$6"
	echo -e "\e[34m Total tests \e[36m$curentTest \e[34mof \e[36m$totalTests"
	echo -e "\e[39m"
	curentTest=$((curentTest+1))
}

curentTest=1
totalTests=$(($((6*6*4*3))*$loop))
date1=$(date +"%s")
test "POKER" "/data/_Collections/POKER/Export.txt" "/data/_Collections/POKER/Queries.txt"
test "TIGER" "/data/_Collections/TIGER/Export.txt" "/data/_Collections/TIGER/Queries.txt"
test "KDDCUP" "/data/_Collections/KDDCUP/Export.txt" "/data/_Collections/KDDCUP/Queries.txt"
#test "USCENSUS" "/data/_Collections/US_CENSUS/Export.txt" "/data/_Collections/US_CENSUS/Queries.txt"
#test "XML" "/data/_Collections/XML_COLLECTION/Export_15_8.txt" "/data/_Collections/XML_COLLECTION/Queries_15_8.txt"
#test "XMARK" "/data/_Collections/XMARK/Export.txt" "/data/_Collections/XMARK/Queries.txt"
#test "USAROADS" "/data/_Collections/USA_ROADS/Export.txt" "/data/_Collections/USA_ROADS/Queries.txt"
#test "METEO" "/data/_Collections/METEO/Export57.txt" "/data/_Collections/METEO/Queries57.txt"

date2=$(date +"%s")
diff=$(($date2-$date1))
echo "$(($diff / 60)) minutes and $(($diff % 60)) seconds elapsed."