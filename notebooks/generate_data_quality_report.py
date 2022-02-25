from num_posts_summary_script import *
from missing_file_report_script import *
from sentiment_graph_script import *
from fpdf import FPDF
import datetime

def generate_report_files(start_year, end_year, geo_dir, sent_dir, in_dir, pipeline_dir, out_dir, cities, city_group, internal=False):
    """
    :param start_year: int, Ex. 2021
    :param end_year: int
    :param geo_dir: str, the directory to geography data
    :param sent_dir: str, the directory to sentiment data
    :param in_dir: str, the directory to code
    :param pipeline_dir: str, the directory to which intermediate files will be stored
    :param out_dir: str, the directory to which we will save the output
    :param cities: str, the directory to which we will save the output
    :param city_group: str, name of the group of cities (Ex. "top 5 US cities")
    :param internal: boolean, if True, include extra info on missing data

    save pdf report to out_dir, with the file name "data_report_[start_year]_to_[end_year].pdf"
    """
    num_posts_graphs = []
    sentiment_graphs = []
    missing_files = []
    empty_files = []
    corrupted_files = []

    for year in range(start_year, end_year + 1):

        generate_daily_num_posts_df_year(year, geo_dir, sent_dir, pipeline_dir)

        graph_file_name = generate_daily_num_posts_graph_year(year, pipeline_dir, out_dir)
        num_posts_graphs.append(graph_file_name)

        for month in range(1, 13):
            generate_daily_sentiment_avg_and_num_posts_by_month_csv(cities, year, month, city_group, pipeline_dir)

        start_date = datetime.date(year, 1, 1)
        end_date = datetime.date(year, 12, 31)
        generate_daily_sentiment_graph(start_date, end_date, city_group, in_dir, out_dir,
                                       remove_corrupted_data=True, corrupt_data_dir=pipeline_dir)
        for city in cities:
            sentiment_graphs.append("".join([out_dir, city,"_daily_sentiment_", start_year, "_to_", end_year, ".png"]))

        if internal:
            for folder in ["geography", "sentiment"]:
                saving_list_of_files(year, folder, geo_dir, sent_dir, out_dir)
                missing_files.append(''.join([out_dir, "missing_files_", str(year), "_", folder, ".csv"]))
                empty_files.append(''.join([out_dir, "empty_files_", str(year), "_", folder, ".csv"]))
                corrupted_files.append(''.join([out_dir, "corrupted_files_", str(year), "_", folder, ".csv"]))

        result = {"num_posts_graphs": num_posts_graphs,
                  "sentiment_graphs": sentiment_graphs,
                  "missing_files": missing_files,
                  "empty_files": empty_files,
                  "corrupted_files": corrupted_files
                  }

        return result


def generate_pdf_report(inputs, internal=False):
    """
    need dict
    {"num_posts_graphs": [list of file paths], "sentiment_graphs": [list of file paths], "missing_files": [file paths]}
    @return:
    """
    pdf = FPDF()

    # number of posts section
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 10, 'Number of Posts Summary Graphs')
    x, y = 40, 30
    for i in range(len(inputs["num_posts_graphs"])):
        if i > 0 and i % 3 == 0:
            pdf.add_page()
            y = 30
        pdf.image(inputs["num_posts_graphs"][i], x=40, y=y, w=150, h=75, type='', link='')
        y += 80

    # sentiment graph section
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 10, 'Sentiment Graphs')
    x, y = 40, 30
    for i in range(len(inputs["sentiment_graphs"])):
        if i > 0 and i % 3 == 0:
            pdf.add_page()
            y = 30
        pdf.image(inputs["sentiment_graphs"][i], x=40, y=y, w=100, h=75, type='', link='')
        y += 80

    # if internal:


if __name__ == '__main__':


    pdf = FPDF()

    # number of posts section
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 10, 'Number of Posts Summary Graphs')
    pdf.image("../../summary_graphs/2012_plot.png", x=40, y=30, w=150, h=75, type='', link='')
    pdf.image("../../summary_graphs/2013_plot.png", x=40, y=110, w=150, h=75, type='', link='')
    pdf.image("../../summary_graphs/2014_plot.png", x=40, y=190, w=150, h=75, type='', link='')

    # sentiment scores
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 10, 'Sentiment Graphs')
    pdf.image("../../sentiment_graphs/2019_Cook_graph_clean.png", x=40, y=30, w=100, h=75, type='', link='')
    pdf.image("../../sentiment_graphs/2019_Harris_graph_clean.png", x=40, y=110, w=100, h=75, type='', link='')
    pdf.image("../../sentiment_graphs/2019_Orange_graph_clean.png", x=40, y=190, w=100, h=75, type='', link='')

    pdf.output('../output/tuto1.pdf', 'F')




    for year in range(2012, 2013):
        # geo_dir = "/srv/data/twitter_geography/"
        # sent_dir = "/srv/data/twitter_sentiment/"
        # pipeline_dir = "/home/sirenay/2_pipeline"
        # generate_daily_num_posts_df_year(year, geo_dir, sent_dir, pipeline_dir)

        # out_dir = "/home/sirenay/3_out/"
        # generate_report_files(year, year, )



