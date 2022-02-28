from fpdf import FPDF
from settings import *


def generate_report(years, areas, internal=False):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 10, 'Number of Posts Summary Graphs')

    counter = 0
    for year in years:
        for area in areas:
            print(year)
            if counter > 0 and counter % 3 == 0:
                pdf.add_page()
                counter = 0
            image_dir = "".join([DIR_STORE, "daily_num_posts_graph_in_", area, "_", str(year), ".png"])
            pdf.image(image_dir, x=40, y=50 + counter * 80, w=150, h=100, type='', link='')
            counter += 1

    pdf.add_page()
    pdf.cell(40, 10, 'Sentiment Graphs')

    counter = 0
    for year in years:
        for area in areas:
            print(year)
            if counter > 0 and counter % 3 == 0:
                pdf.add_page()
                counter = 0
            image_dir = "".join([DIR_STORE, area, "_daily_sentiment_", str(year), ".png"])
            pdf.image(image_dir, x=40, y=50 + counter * 80, w=150, h=100, type='', link='')
            counter += 1

    if internal:
        pass

    pdf.output("".join([DIR_OUTPUT, str(years[0]), "_", areas[0], "_report.pdf"]), 'F')