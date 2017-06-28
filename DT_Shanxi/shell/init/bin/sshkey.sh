#!/bin/bash
user=$1
node=""
wordCount=0;
while read line
do
for word in $line;
do
firstChar=${word:0:1} 
wordCount=$[$wordCount+1]
isOut=`echo $[$wordCount%2]`
if [ $isOut == 0 ];then
node="$node $word"
break
fi
done
done < /etc/hosts

node=($node)  # hostname for each node
username=root #username to be interconnected
homename=home/$user	# home dir, i.e. home/zhangyang

for((i=0; i<${#node[*]}; i++))
do
        echo "ssh -t $username@${node[i]} 'groupadd $user; useradd -g $user $user; passwd $user'"
        #ssh $username@${node[i]} "groupadd $user"
        ssh -t $username@${node[i]} "groupadd $user; useradd -g $user $user; passwd $user;"
done

for((i=0; i<${#node[*]}; i++))
do
        ssh $user@${node[i]} 'chmod 700 /$homename; ssh-keygen -t rsa; chmod 700 ~/.ssh; chmod 600 ~/.ssh/authorized_keys'
done
#cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

echo "batch authorized_keys created..."
echo "start scp..."

#scp node003:/$homename/.ssh/authorized_keys /$homename/.ssh/node003.key
for((i=0; i<${#node[*]}; i++))
do
        scp ${node[i]}:/$homename/.ssh/id_rsa.pub /$homename/.ssh/${node[i]}.key
        echo "scp from ${node[i]} finished..."
done

echo "append key to authorized_keys..."
for((i=0; i<${#node[*]}; i++))
do
        cat /$homename/.ssh/${node[i]}.key >> /$homename/.ssh/authorized_keys
        echo "append ${node[i]}.key finished..."
done

echo "append all key finished..."
loop=${#node[*]}
let subloop=loop-1
echo "starting scp complete authorized_keys to ${node[1]}~${node[subloop]}"
for((i=1; i<${#node[*]}; i++))
do
        scp /$homename/.ssh/authorized_keys ${node[i]}:/$homename/.ssh/authorized_keys
        echo "scp to ${node[i]} finished..."
done
echo "scp all nodes finished..."

# delete intermediate files
rm -rf /$homename/.ssh/*.key
echo "all configuration finished..."

