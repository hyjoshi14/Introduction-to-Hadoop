from mrjob.job import MRJob
from mrjob.step import MRStep

class FilePath(MRJob):
    def mapper(self,_,line):
        data = line.strip().split()
        if len(data) == 10:
            path = data[6]
            file = path.split('/')[-1]
            if file.startswith(r'http://www.the-associates.co.uk'):
                file = file.replace(r'http://www.the-associates.co.uk','')
            if file == u'':
                pass
            else:
                yield(file,1)

    def reducer(self,file,counts):
        yield(file,sum(counts))

    def mapper2(self,file,count):
        yield(1,{'file':file,'count':count})

    def reducer2(self,key,info):
        files = []
        counts = []
        for data in info:
            files.append(data['file'])
            counts.append(data['count'])
        FileCountList = zip(files,counts)
        print sorted(FileCountList, key = lambda x: x[-1], reverse = True)[:20]

    def steps(self):
        return[
        MRStep(mapper = self.mapper,reducer = self.reducer),
        MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
    FilePath.run()
