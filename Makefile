download-data:
	./src/utils/download_dataset.sh

prep:
	pipenv run python3 src/utils/prep_data_01_cast_dtype.py
	pipenv run python3 src/utils/prep_data_02_repartition.py

run:
	pipenv run python3 src/utils/generate_run_script.py
	bash run.sh
