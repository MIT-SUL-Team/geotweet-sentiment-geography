from num_posts_summary_script import *
from missing_file_report_script import *
from sentiment_graph_script import *
from fpdf import FPDF


def generate_report(start_date, end_date, in_dir, pipeline_dir, out_dir, internal=False):
    """
    :param start_date: str, in the form "yyyy-mm-dd" (Ex. "2022-02-22")
    :param end_date: str, the the form "yyyy-mm-dd"
    :param in_dir: str, the directory to code
    :param pipeline_dir: str, the directory to which intermediate files will be stored
    :param out_dir: str, the directory to which we will save the output
    :param internal: boolean, if True, include extra info on missing data

    save pdf report to out_dir, with the file name "data_report_[start_date]_to_[end_date].pdf"
    """

    # TODO:
    # 
    # 2) create fake pdf (figure out pdf technology)
    # 3) incorporate methods into generate_report, run for one month
    # 4) run for whole history
    return None




if __name__ == '__main__':

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 10, 'Hello World!')
    # pdf.image("/home/sirenay/3_out/daily_num_posts_graph_2012.png", x=None, y=None, w=600, h=300, type='', link='')
    pdf.output('/home/sirenay/3_out/tuto1.pdf', 'F')


    for year in range(2012, 2013):
        geo_dir = "/srv/data/twitter_geography/"
        sent_dir = "/srv/data/twitter_sentiment/"
        pipeline_dir = "/home/sirenay/2_pipeline"
        # generate_daily_num_posts_df_year(year, geo_dir, sent_dir, pipeline_dir)

        out_dir = "/home/sirenay/3_out/"
        # generate_daily_num_posts_graph_year(year, pipeline_dir, out_dir)

        # for folder in ["geography", "sentiment"]:
            # saving_list_of_files(year, folder, geo_dir, sent_dir, pipeline_dir)



