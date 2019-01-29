import shlex, subprocess
cmd = 'hadoop fs -test -d $/basic_data/icdc/algorithms/20190115/icdc_0/data__ff0f1b40_5207_4f3c_83d0_8f03b7185372'
#cmd = raw_input("shell:")
args = shlex.split(cmd)
output,error = subprocess.Popen(args,stdout = subprocess.PIPE, stderr= subprocess.PIPE).communicate()
print(output)