.PHONY: worker
# Followed by an integer argument, run the map reduce worker with WOKRER_ID environment variable set to the argument.
worker-%:
	INPUT_DIRECTORY=input/worker-$* OUTPUT_DIRECTORY=output/worker-$* cargo run -p map-reduce-worker

.PHONY: chunks
# Run the chunkify program. Drops input files into the input directory, replicated for each worker.
chunks:
	cargo run -p chunkify

.PHONY: master
# Run map reduce master
master:
	cargo run -p map-reduce-master