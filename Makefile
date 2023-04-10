download-data:
	./src/utils/download_dataset.sh

prep-base:
	@if [ ! -d "data/prep" ]; then \
		pipenv run python3 src/utils/prep_data_base_cast_dtype.py; \
	fi

run-expt1-window-on-single-partition:
	pipenv run python3 src/utils/expt1/prep_data.py
	pipenv run python3 src/utils/expt1/generate_run_script.py
	bash run.sh
	pipenv run python3 src/utils/expt1/create_viz.py


run-expt2-window-on-multiple-partition:
	pipenv run python3 src/utils/expt2/prep_data.py
	pipenv run python3 src/utils/expt2/generate_run_script.py
	bash run.sh
	pipenv run python3 src/utils/expt2/create_viz.py
