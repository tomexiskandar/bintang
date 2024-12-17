import time
import datetime

def ttic():
    global _start_time 
    _start_time = time.time()

def ttac():
    t_sec = round(time.time() - _start_time)
    (t_min, t_sec) = divmod(t_sec,60)
    (t_hour,t_min) = divmod(t_min,60) 
    print('Time passed: {}hour:{}min:{}sec'.format(t_hour,t_min,t_sec))
    #return (t_hour,t_min,t_sec)

def tic():
    global _start_time 
    _start_time = datetime.datetime.now()

def tac():
    print('duration: {}'.format(datetime.datetime.now()-_start_time))
    return datetime.datetime.now()-_start_time
        
# >>> import datetime
# >>> first_time = datetime.datetime.now()
# >>> later_time = datetime.datetime.now()
# >>> difference = later_time - first_time

class Computer(object):
    def boot_message(self):
        return 'I am a computer'

class AppleComputer(Computer):
    def boot_message(self):
        return super(AppleComputer, self).boot_message() + ' with a really shiny logo'
    
c = AppleComputer()
#print(c.boot_message())