cd "$(dirname $0)"
source ../common/init.sh

./test_c2b/run_all.sh && \
./test_bro/run_all.sh

if [ $? -eq 0 ]
then
    echo "[OK]"
    exit 0
else
    echo "[ERROR]"
    exit -1
fi
