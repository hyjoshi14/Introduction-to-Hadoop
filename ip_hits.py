from mrjob.job import MRJob

class IPHits(MRJob):

    def mapper(self,_,line):
        data = line.strip().split()
        if len(data) == 10:
            ip_address = data[0]
            if ip_address == '10.99.99.186':
                yield(ip_address,1)

    def reducer(self,ip_address,hits):
        yield(ip_address,sum(hits))

if __name__ == '__main__':
    IPHits.run()
