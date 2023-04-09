download-data:
	./src/utils/download_dataset.sh

prep-base:
	@if [ ! -d "data/prep" ]; then \
		pipenv run python3 src/utils/prep_data_base_cast_dtype.py; \
	fi

prep-ex1: prep-base
	pipenv run python3 src/utils/prep_data_ex1.py
run-ex1-window-on-single-partition: prep_ex1
	pipenv run python3 src/utils/generate_run_script.py
	bash run.sh

visualize:
	pipenv run python3 src/utils/create_viz.py
