import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import time

class rfi_challenge:

    def __init__(self):
        self.tones=[] #save all added tones to check false negative/postive ratio

    """Function to generate background noise in the range for observation of dimension (xDim, yDim)
    Assuming a standard observation will have normally distributed data (with a mean of mean and stdDev of stdDev) if it has no interference
    xDim and yDim should be integers
    mean and stdDev must be valid foat32"""
    def generate_data (self, xDim, yDim, mean, stdDev):

        self.xDim = xDim
        self.yDim = yDim
        self.centre = mean
        self.knownRFI = np.zeros(shape = (101, yDim, xDim), dtype = np.bool_)
        self.detection_rates = np.zeroes(shape = (101, yDim, xDim))
        rs = np.random.RandomState(int(time.time()))
        #self.data = rs.rand(yDim, xDim) * diff + minVal
        self.data = rs.normal(loc = mean, scale = stdDev, size = (yDim, xDim))
        self.stdDevX = np.std(self.data, axis=1)
        self.stdDevY = np.std(self.data, axis=0)
        self.stdDev = stdDev
        print self.stdDev


    """Add a constant wave tone on channel chan from startTime to endTime with power equal to power"""
    def add_CW_tone (self, chan, startTime, endTime, power, level = 0):
        self.tones.append({"chan":chan,"startTime":startTime,"endTime":endTime, "power":power})
        self.data[startTime:endTime, chan] = self.data[startTime:endTime, chan] + power
        self.knownRFI[0][startTime:endTime, chan].fill(True)
        if (level != 0):
            self.knownRFI[level][startTime:endTime, chan].fill(True)

    """Add numTones random CW tones, all tones have a power level equal to a multiple of the
    standard deviation of the noise
    numTones is the number of random tones to add
    variability is the maximum multiple of std Devs to set as a power level"""
    def add_random_tones(self, numTones, variability):
        devs = np.random.randint(1, high = variability, size = numTones) #power multiplier
        chans = np.random.randint(0, high = self.xDim, size = numTones) #channels
        startStop =np.random.randint(0, high = self.yDim, size = (2,numTones))
        mins = np.min(startStop, axis = 0) #startTimes
        maxs = np.max(startStop, axis = 0) #endTimes
        for i in range(numTones):
            self.add_CW_tone(chans[i], mins[i], maxs[i], devs[i]*self.stdDev, level = devs[i])


    def display_data(self):
        print"Plotting"
        plt.imshow(self.data, interpolation = 'none', cmap = cm.Blues)
        plt.show()

    def get_rfi(self, data,sigma=4):
        """ Get the rfi for the middle of the window 
        Data is assumed to be a 2D array
        and sigma is the standard deviation that is used"""
        mid = data.shape[0]//2+1   # mid point of window
        med = np.median(data[:,:])

        mad = np.median(np.abs(data[:] - med),axis=0)

        mad_limit = sigma/1.4826 # see relation to standard deviation
        return (data[mid]  > mad_limit*mad + med ) + (data[mid]  < -mad_limit*mad + med )

    def TD_median_filter (self, window=10, sigma = 4):
        rfi = np.zeros(self.data.shape)
        for t in range(window,self.data.shape[0]-window) :  rfi[t,:] = self.get_rfi(self.data[t-window:t+window+1])
        self.TD_rfi_mask = rfi

    def FD_median_filter (self, window=10, sigma = 6):
        rfi = np.zeros((self.data.shape[1],self.data.shape[0]))
        self.data = np.transpose(self.data)
        for t in range(window,self.data.shape[0]-window) :  rfi[t,:] = self.get_rfi(self.data[t-window:t+window+1], sigma = sigma)
        self.data = np.transpose(self.data)
        self.FD_rfi_mask = np.transpose(rfi)

    def calc_detection_rates (self):
        true_pos = np.logical_and(self.FD_rfi_mask,self.knownRFI[0])
        false_






if __name__ == "__main__":
    rc = rfi_challenge()
    rc.generate_data(1000,1500, 0,1)
    rc.add_CW_tone(50, 25, 75, 100)
    rc.add_random_tones(100,100)
    rc.TD_median_filter()
    rc.FD_median_filter()
    masked = np.ma.array(rc.data, mask = TD_rfi_mask)

    fig = plt.figure()
    one = fig.add_subplot(2,1,1)
    one.imshow(rc.knownRFI[0], interpolation = 'none', cmap = cm.Blues)
    two = fig.add_subplot(2,1,2)
    two.imshow(rc.FD_rfi_mask, interpolation = 'none', cmap = cm.Blues)

    #plt.imshow(masked, interpolation = 'none', cmap = cm.Blues)
    plt.show()
    # rc.display_data()

    # fig, ax = plt.subplots()
    # im = ax.pcolor(rc.data, cmap=cm.Blues)
    # fig.colorbar(im)

    # ax.patch.set_hatch('x')

    # plt.show()
