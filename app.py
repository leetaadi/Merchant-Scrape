# app.py
from flask import Flask, render_template, request, send_file, jsonify
import os
import threading
import uuid
from scraper import run_scraper

app = Flask(__name__)

# Store progress information
progress_data = {}

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        query = request.form.get('query')
        number = request.form.get('number')
        if query and number:
            # Generate unique task ID
            task_id = str(uuid.uuid4())
            
            # Initialize progress
            progress_data[task_id] = {
                "status": "starting",
                "progress": 0,
                "message": "Initializing...",
                "filename": None,
                "result": None
            }
            
            # Start scraping in background thread
            thread = threading.Thread(
                target=run_scraping_task,
                args=(query, int(number), task_id)
            )
            thread.start()
            
            return jsonify({"task_id": task_id})
    
    return render_template('index.html')

@app.route('/progress/<task_id>')
def progress(task_id):
    if task_id in progress_data:
        return jsonify(progress_data[task_id])
    return jsonify({"error": "Invalid task ID"}), 404

@app.route('/download')
def download():
    filepath = request.args.get('file')
    return send_file(filepath, as_attachment=True)

def run_scraping_task(query, number, task_id):
    try:
        # Update progress
        progress_data[task_id] = {
            "status": "collecting",
            "progress": 10,
            "message": "Collecting business links...",
            "filename": None,
            "result": None
        }
        
        # Run the scraper with a progress callback
        def update_progress(progress, message):
            progress_data[task_id] = {
                "status": "running",
                "progress": progress,
                "message": message,
                "filename": progress_data[task_id].get("filename"),
                "result": progress_data[task_id].get("result")
            }
        
        filename, result = run_scraper(
            query, 
            number, 
            progress_callback=update_progress
        )
        
        # Final update
        progress_data[task_id] = {
            "status": "completed",
            "progress": 100,
            "message": "Scraping completed!",
            "filename": filename,
            "result": result
        }
    except Exception as e:
        progress_data[task_id] = {
            "status": "error",
            "progress": 100,
            "message": f"Error: {str(e)}",
            "filename": None,
            "result": None
        }

if __name__ == '__main__':
    if not os.path.exists("output"):
        os.makedirs("output")
    app.run(debug=True)