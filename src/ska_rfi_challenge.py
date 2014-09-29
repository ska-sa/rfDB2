
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import time
import argparse
import sys

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
        rs = np.random.RandomState(int(time.time()))
        self.data = rs.normal(loc = mean, scale = stdDev, size = (yDim, xDim))
        self.stdDevX = np.std(self.data, axis=1)
        self.stdDevY = np.std(self.data, axis=0)
        self.stdDev = stdDev


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
    max_level is the maximum multiple of std Devs to set as a power level
    min_level is the minimum multiple of std Devs to set as a power level"""
    def add_random_tones(self, numTones, min_level, max_level):
        self.knownRFI = np.zeros(shape = (max_level - min_level + 1, self.yDim, self.xDim), dtype = np.bool_)
        self.detection_rates = np.zeros(shape = (max_level - min_level + 1 + 1))
        devs = np.random.randint(min_level, high = max_level + 1, size = numTones) #power multiplier
        chans = np.random.randint(0, high = self.xDim, size = numTones) #channels
        startStop =np.random.randint(0, high = self.yDim, size = (2,numTones))
        mins = np.min(startStop, axis = 0) #startTimes
        maxs = np.max(startStop, axis = 0) #endTimes
        for i in range(numTones):
            self.add_CW_tone(chans[i], mins[i], maxs[i], devs[i]*self.stdDev, level = devs[i] - min_level)


    def display_data(self):
        print"Plotting"
        plt.imshow(self.data, interpolation = 'none', cmap = cm.Blues)
        plt.show()

    """ Get the rfi for the middle of the window of data selected using Median Absolut Distance (MAD) as an estimator 
    Data is assumed to be a 2D array
    If values in window differ from MAD estimator then they are flagged as RFI
    essentially data which is greater than sigma*stdDev away from the median will be flagged
    Median absolute distance is a more robust estimator for standard deviation than Mean Absolute Distance"""
    def get_rfi(self, data,sigma=4, axis = 0):

        mid = data.shape[axis]//2+1   # mid point of window
        med = np.median(data[:,:])

        mad = np.median(np.abs(data[:] - med),axis=axis)

        mad_limit = sigma/1.4826 # see relation to standard deviation (http://en.wikipedia.org/wiki/Median_absolute_deviation)
        if (axis == 0):
            return (data[mid]  > mad_limit*mad + med ) + (data[mid]  < -mad_limit*mad + med )
        elif (axis == 1):
            return (data[:,mid]  > mad_limit*mad + med ) + (data[:,mid]  < -mad_limit*mad + med )

    """Move window through data in time domain"""
    def TD_median_filter (self, window=10, sigma = 4):
        rfi = np.zeros(self.data.shape)
        for t in range(window,self.data.shape[0]-window) :  rfi[t,:] = self.get_rfi(self.data[t-window:t+window+1])
        self.TD_rfi_mask = rfi

    """Move window through data in Frequency Domain"""
    def FD_median_filter (self, window = 10, sigma = 6):
        rfi = np.zeros(self.data.shape)
        for t in range(window,self.data.shape[1]-window) :  rfi[:,t+1] = self.get_rfi(self.data[:,t-window:t+window+1], sigma = sigma, axis = 1)
        self.FD_rfi_mask = rfi


    """Calculate detection rates for a given mask"""
    def calc_detection_rates (self, mask, min_level, disp_l = True):
        self.true_pos = np.logical_and(mask,self.knownRFI[0]) #True detection where known RFI and detected RFI agree
        self.false_pos = mask - self.true_pos
        self.true_neg = np.logical_and(np.logical_not(mask), np.logical_not(self.knownRFI[0]))
        self.false_neg = np.logical_not(mask) - self.true_neg
        n_false_neg = np.sum(self.false_neg)
        n_false_pos = np.sum(self.false_pos)
        n_true_pos = np.sum(self.true_pos)
        n_true_neg = np.sum(self.true_neg)
        total_rfi = np.sum(self.knownRFI[0])
        total_clear = np.product(self.knownRFI[0].shape) - total_rfi

        print "total_rfi = %i"%total_rfi
        print "total_clear = %i"%total_clear

        print "n_false_neg = %i"%n_false_neg
        print "n_false_pos = %i"%n_false_pos
        print "n_true_pos = %i"%n_true_pos
        print "n_true_neg = %i"%n_true_neg
        print "n_false_pos/n_false_neg = %f"%(float(n_false_pos)/n_false_neg)
        print "n_true_pos/n_false_pos = %f"%(float(n_true_pos)/n_false_pos)
        print "n_true_neg/n_false_neg = %f"%(float(n_true_neg)/n_false_neg)
        print "RFI detected = %.2f%%"%(float(n_true_pos)/total_rfi * 100)
        print "Data erroneously flagged = %.2f%%"%(float(n_false_pos)/total_clear * 100)

        if (disp_l):
            for i in range (1,self.knownRFI.shape[0]):
                total_rfi = np.sum(self.knownRFI[i])
                if (total_rfi > 0):
                    true_pos = np.logical_and(mask, self.knownRFI[i])
                    n_true_pos = np.sum(true_pos)  
                    print "RFI with power sigma*%i were detected %.2f%% of the time"%(min_level + i - 1,float(n_true_pos)/total_rfi * 100)



if __name__ == "__main__":

    #---------------------------------------------------------PARSE ARGS------------------------------------------------------------------------------------------------------
    parser = argparse.ArgumentParser(description='Set up test run of RFI algorithm')
    parser.add_argument('-x','--xDim', default = 1000, help="X dimension of synthesised data", dest = 'xDim', type=int)
    parser.add_argument('-y','--yDim', default = 1000, help="Y dimension of synthesised data", dest = 'yDim', type=int)
    parser.add_argument('-n','--nRFI', default = 100, help="Number of RFI signals to randomly introduce", dest ='nRFI', type=int)
    parser.add_argument('-l','--max_level', default = 10, help="Max power level of RFI to introduce, RFI at level n will have a power of n*stdDev\
                                                            where stdDev is the standard deviation of the generated data", dest='max_level', type=int)
    parser.add_argument('-m','--min_level', default = 1, help="Minimum power level of RFI to introduce RFI at level n will have a power of n*stdDev\
                                                            where stdDev is the standard deviation of the generated data", dest='min_level', type=int)
    parser.add_argument('--display_levels', help="Display the percentage of RFI at each power level which was detected", dest='display_levels', action='store_true')
    parser.add_argument('--no-display_levels', help="Do not display the percentage of RFI at each power level which was detected", dest='display_levels', action='store_false')
    parser.add_argument('-w','--window_length', default = 10, help="Set length of window for Median Absolute Distance, a longer window gives more accurate results\
                                                                    and increases computation time. Default value 10", dest='window',type=int)
    parser.add_argument('-s','--sigma_threshold', default = 6, help="Set the threshold level for RFI detections in Median Absolute Distance Algorithm, lower thresholds \
                                                                    mean more false positives and less false negatives. Default value 6", dest='sigma', type=int)
    parser.add_argument('--plot_data', help="Plot data and RFI masks with matplotlib. Default true", dest='plot_data', action='store_true')
    parser.add_argument('--no-plot_data', help="Do not plot data and RFI masks with matplotlib. Default true", dest='plot_data', action='store_false')
    parser.add_argument('--with_TD', help="Perform RFI detection in Time Domain as well as frequency domain and display results", dest='with_TD', action = 'store_true')
    parser.add_argument('--without_TD', help=" Do not perform RFI detection in Time Domain", dest='with_TD', action = 'store_false')
    parser.set_defaults(plot_data=False, display_levels=True, with_TD=False)
    args = parser.parse_args()
    print args
    #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    rc = rfi_challenge()
    rc.generate_data(args.xDim,args.yDim, 0,1)
    rc.add_random_tones(args.nRFI,args.min_level, args.max_level)
    rc.TD_median_filter(window=args.window, sigma=args.sigma)
    rc.FD_median_filter(window=args.window, sigma=args.sigma)
    masked = np.ma.array(rc.data, mask = rc.FD_rfi_mask)
    print("------------------Frequency Domain RFI Detection-----------------------")
    rc.calc_detection_rates(rc.FD_rfi_mask, args.min_level, disp_l = args.display_levels)
    print("-----------------------------------------------------------------------")

    if(args.plot_data):
        fig = plt.figure()
        one = fig.add_subplot(3,2,1)
        one.imshow(rc.data, interpolation = 'none', cmap = cm.Blues)
        one.set_title("Raw Data")
        two = fig.add_subplot(3,2,3)
        two.imshow(rc.knownRFI[0], interpolation = 'none', cmap = cm.Blues)
        two.set_title("Known RFI")
        three = fig.add_subplot(3,2,4)
        three.imshow(rc.FD_rfi_mask, interpolation = 'none', cmap = cm.Blues)
        three.set_title("Detected RFI")
        four = fig.add_subplot(3,2,5)
        four.imshow(rc.true_pos, interpolation = 'none', cmap = cm.Blues)
        four.set_title("True Positives")
        five = fig.add_subplot(3,2,6)
        five.imshow(rc.false_pos, interpolation = 'none', cmap = cm.Blues)
        five.set_title("False Positives")

        plt.show()

    if (args.with_TD):
        print("---------------------Time Domain RFI Detection-------------------------")
        rc.calc_detection_rates(rc.TD_rfi_mask, args.min_level, disp_l = args.display_levels)
        print("-----------------------------------------------------------------------")
        if(args.plot_data):
            fig = plt.figure()
            one = fig.add_subplot(3,2,1)
            one.imshow(rc.data, interpolation = 'none', cmap = cm.Blues)
            one.set_title("Raw Data")
            two = fig.add_subplot(3,2,3)
            two.imshow(rc.knownRFI[0], interpolation = 'none', cmap = cm.Blues)
            two.set_title("Known RFI")
            three = fig.add_subplot(3,2,4)
            three.imshow(rc.TD_rfi_mask, interpolation = 'none', cmap = cm.Blues)
            three.set_title("Detected RFI")
            four = fig.add_subplot(3,2,5)
            four.imshow(rc.true_pos, interpolation = 'none', cmap = cm.Blues)
            four.set_title("True Positives")
            five = fig.add_subplot(3,2,6)
            five.imshow(rc.false_pos, interpolation = 'none', cmap = cm.Blues)
            five.set_title("False Positives")

            plt.show()