getRedditData: getRedditData.py comment.py submission.py
	python getRedditData.py

processText: processText.py comment.py submission.py
	python processText.py

clean:
	rm -f data/*
