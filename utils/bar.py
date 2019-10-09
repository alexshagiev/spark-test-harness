from tqdm import trange
from time import sleep
t = trange(100, desc='Bar desc', leave=True)
for i in t:
    t.set_description("Bar desc (file %i)" % i)
    t.refresh() # to show immediately the update
    sleep(0.01)