Implemented features:
	First, this app can build separate graph for each chromosome from vcf file (reference required) and accumulate information about mutations in same chromosome from 
multiple vcf-files. We will try to build graph with minimum vertices. That means that, for example, if we had mutation a -> aaaaa and then
we see mutation a -> aacaa there will be added only one vertex 'c' and it will be attached to existing allele instead of creating whole new branch "aacaa".
	Second, existing graph with accumulated mutation from different vcfs may be translated back to vcf format (for now translation supports
only mutations without any specific information being translated). It's also possible to specify which chromosomes you would like to include in vcf.
Also, there is translation to .dot format for interaction with graph visualization programs. 
