
from bs4 import BeautifulSoup
import requests
from time import sleep
import datetime
import multiprocessing as mp


class VidyutPravah:

    def __init__(self):
        self.base_url = "http://www.vidyutpravah.in"
        self.soup = VidyutPravah.get_soup(self.base_url)

    @staticmethod
    def get_text_from_url(url):
        webpage = requests.get(url)
        text = webpage.text
        return text

    @staticmethod
    def make_soup_from_text(text):
        soup = BeautifulSoup(text, 'lxml')
        return soup

    @staticmethod
    def get_soup(url):
        text = VidyutPravah.get_text_from_url(url)
        soup = VidyutPravah.make_soup_from_text(text)
        return soup

    def get_all_state_links(self):
        links = self.soup.find_all("a")
        state_links = []
        for link in links:
            state_link = link.attrs.get('href')
            try:
                if state_link.startswith("/state-data"):
                    state_links.append(state_link)
            except AttributeError:
                pass
        return state_links

    @staticmethod
    def get_state_soup(state_url):
        return VidyutPravah.get_soup(state_url)


class VidyutPravahState(VidyutPravah):
    def __init__(self, state_url):
        VidyutPravah.__init__(self)
        self.state_url = self.get_state_url(state_url)
        self.state_soup = VidyutPravahState.get_soup(self.state_url)

    def get_state_url(self, state_partial_url):
        return self.base_url + state_partial_url

    def get_start_time(self, soup):
        to_time = soup.find_all("b")
        return to_time[0].getText().split("-")[0].strip()

    def get_end_time(self, soup):
        from_time = soup.find_all("b")
        return from_time[0].getText().split("-")[1].strip()

    def get_exchange_price_current(self, soup):
        cep = soup.find_all("span", class_="value_ExchangePrice_en value_StateDetails_en")
        return cep[0].getText().replace(u'\xa0', u' ').split()[0]

    def get_exchage_price_yesterday(self, soup):
        yep = self.state_soup.find_all("span", class_="value_PrevExchangePrice_en value_StateDetails_en")
        return yep[0].getText().replace(u'\xa0', u' ').split()[0]

    def get_prev_power_purchased(self, soup):
        ppp = soup.find_all("span", class_="value_PrevPowerPurchase_en value_StateDetails_en")
        return ppp[0].getText().replace(u'\xa0', u' ').split()[0]

    def get_current_power_purchased(self, soup):
        cpp = soup.find_all("span", class_="value_PowerPurchase_en value_StateDetails_en")
        return cpp[0].getText().replace(u'\xa0', u' ').split()[0]

    def state_demand_met_current(self, soup):
        demand = soup.find_all("span", class_="value_DemandMET_en value_StateDetails_en")
        return demand[0].getText().replace(u'\xa0', u' ').split()[0]

    def state_demand_met_yesterday(self, soup):
        demand = soup.find_all("span", class_="value_PrevDemandMET_en value_StateDetails_en")
        return demand[0].getText().replace(u'\xa0', u' ').split()[0]

    def shortage_yesterday_during_peak(self, soup):
        shortage = soup.find_all("span", class_="value_PeakDemand_en value_StateDetails_en")
        return shortage[0].getText().replace(u'\xa0', u' ').split()[0]

    def shortage_yesterday_energy(self, soup):
        shortage = soup.find_all("span", class_="value_TotalEnergy_en value_StateDetails_en")
        return shortage[0].getText().replace(u'\xa0', u' ').split()[0]

    def get_values(self):
        # refresh state soup
        state_soup = VidyutPravahState.get_soup(self.state_url)
        now_date = datetime.datetime.now()
        # .strftime("%y/%m/%d")
        cep = self.get_exchange_price_current(state_soup)  # current exchange price (Rupees)
        yep = self.get_exchage_price_yesterday(state_soup)  # yesterday "        "    "
        start_time = self.get_start_time(state_soup)  # time from (HH:MM)
        end_time = self.get_end_time(state_soup)  # to time  "
        ppp = self.get_prev_power_purchased(state_soup)  # prev power purchased (MW)
        cpp = self.get_current_power_purchased(state_soup)  # current power purchased (MW)
        cd = self.state_demand_met_current(state_soup)  # demand met today (MW)
        yd = self.state_demand_met_yesterday(state_soup)  # demand met yesterday (MW)
        ps = self.shortage_yesterday_during_peak(state_soup)  # shortage during peak (MU)
        stey = self.shortage_yesterday_energy(state_soup)  # total energy shortage (MU)

        return [now_date.strftime("%y/%m/%d"), start_time, end_time, cep, yep, cpp, ppp, cd, yd, ps, stey+"\n"]



class StateTimeStampData():

    def __init__(self, state_url):
        self.state_url = state_url
        self.filename = self.get_state_name_from_url()
        self.state = VidyutPravahState(self.state_url)

    def get_state_name_from_url(self):
        return self.state_url.split("/")[-1]

    def run(self):
        f = open(self.filename+".tsv", "w")
        f.write("DATE(YYYY/MM/DD)\tSTART TIME(HH:MM)\tEND TIME(HH:MM)\tCEP(Rs)\tYEP(Rs)\tCPP(MW)\t"
                "PPP(MW)\tDMT(MW)\tDMY(MW)\tSDP(MU)\tTESY(MU)\n")
        f.close()
        date_time = datetime.datetime.now()
        while date_time.day - datetime.datetime.now().day <= 7:
            data = self.state.get_values()
            f = open(self.filename+".tsv", "a")
            f.write("\t".join(data))
            f.close()
            sleep(900)
        f.close()

def worker(obj):
    return obj.run()

if __name__ == '__main__':
    nation = VidyutPravah()
    state_links = nation.get_all_state_links()
    state_object_pool = [StateTimeStampData(link) for link in state_links]
    pool = mp.Pool(4)
    pool.map(worker, (obj for obj in state_object_pool))
    pool.close()
    pool.join()
