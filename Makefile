download-data:
	./src/utils/download_dataset.sh

run:
	pipenv run python3 src/utils/generate_run_script.py
	bash run.sh
