from mrjob.job import MRJob
import os

class TopCategory(MRJob):
    def mapper(self,_,input):
        data = input.strip().split('\t')
        if len(data) != 6:
            pass
        else:
            category, sales = data[3],float(data[4])
            yield(category,sales)

    def reducer(self,category,sales):
        if category == 'Toys' or category == 'Consumer Electronics':
            tot_sales = 0
            for sale in sales:
                tot_sales += sale
            yield(category,tot_sales)

if __name__ == '__main__':
    TopCategory.run()
