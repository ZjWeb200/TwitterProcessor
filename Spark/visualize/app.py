import ast
from flask import Flask,jsonify,request
from flask import render_template

# Flask get data from spark and send data to browser. These two paths are parallel.
# update_data_from_spark() is for spark-flask.
# showChart() generates the initial html. refresh_hashtag_data() is then called for flask-frontend browser.

app = Flask(__name__)
words = []
counts = []

# handle data from spark. return status code to spark.
# ast package used to parse data.
@app.route('/updateData', methods=['POST'])
def update_data_from_spark():
    global words, counts
    # request is the data from spark
    if not request.form or 'words' not in request.form:
        return "error",400
    words = ast.literal_eval(request.form['words'])
    counts = ast.literal_eval(request.form['counts'])

    print("current words: " + str(words))
    print("current counts: " + str(counts))
    return "success",201

# send refreshed data to frontend as JSON format.
# Ajax call in chart.html
@app.route('/updateChart')
def refresh_hashtag_data():
    global words, counts
    print("current words: " + str(words))
    print("current data: " + str(counts))
    return jsonify(sWords=words, sCounts=counts)

# index page.
@app.route("/")
def showChart():
    global words,counts
    counts = []
    words = []
    return render_template('chart.html', counts=counts, words=words) # generate frontend html skeleton to hold ajax.

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)

