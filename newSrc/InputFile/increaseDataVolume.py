from pathlib import Path

l=Path('C:\\Users\\ajit.ks\\Desktop\\training_dataProject.txt')
new_l=Path('C:\\Users\\ajit.ks\\Desktop\\output\\newTraining_dataProject.txt')

p=open(l)
count = 0
increment = 1
increm = 1
new_data=open(new_l,'a')

def incremental():

    p.seek(0)
    for line_num, i in enumerate(p):
        global increment
        global count
        global increm
        l_digit = int(i.strip(" ").split(" ")[0].split(".")[-1])
        time_hour = int(i.strip(" ").split(" ")[3].split(":")[1])
        if (line_num + 1) % 10 == 0:
            incr = int(l_digit + increment)
            i = i.replace(str(l_digit), str(incr), 1)
            incr_hour = int(time_hour + increm)
            i = i.replace(":0"+str(time_hour), ":0"+str(incr_hour))
        if (line_num + 1) % 9 == 0:
            i = i.replace('GET', 'POST', 1)
        new_data.write(i)
        print(count)
        count = count + 1
        increment = +1
        increm = 1

num_record = int(input("Enter the number of records wanted(in Lakhs): "))
num_acc = int((num_record * 1000))

for i in range(num_acc):
    print(str(i+1))
    incremental()