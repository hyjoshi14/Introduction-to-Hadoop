from mrjob.job import MRJob

class TotSales(MRJob):
    def mapper(self,_,line):
        data = line.strip().split('\t')
        if len(data) == 6:
            sale = data[4]
            yield(1,float(sale))

    def reducer(self,key,sales):
        n_sales = 0
        tot_sales = 0
        for sale in sales:
            n_sales += 1
            tot_sales += sale
        print 'TotSales:{0}, NoOfSales:{1}'.format(tot_sales,n_sales)


if __name__ == '__main__':
    TotSales.run()
