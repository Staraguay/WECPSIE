import math
import time
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.select import Select
import d_utilities as dutils
import st_scraper_wecpsie as dextract
import st_wpse_db_connection as wpsedb



R = '\033[31m'  # red
G = '\033[32m'  # green
C = '\033[36m'  # cyan
W = '\033[0m'  # white
P = '\033[35m'  #morado
Y = '\033[33m'  #amarrilo

now = datetime.now()
today = now.strftime('%Y-%m-%d')


def swing_index_pages(driver):
    """
       Navigator for the search result table in page: Búsqueda de Procesos de Contratación.

       The function crawls over the search result table returned after solving the captcha.
       It scraps the summary information for each row which represents a published proceso de
       contratación pública mediante subasta inversa electronica (PCPSIE).
       The search result table in this page shows at least 20 rows per page. The function handles the
       navigation across all these pages until it reaches the last page, which means it has
       scrapped all the indexed rows information.

       Parameters
       ----------
       driver : webdriver instance to perform crawling operations.

       Notes
       -----
       This function opens a web browser window showing the Búsqueda de Procesos de Contratación web page.
       Wecpsie version ``0.0.1`` does not have a captcha solving utility, so this function provides the user
       a 12 second-window to solve the captcha before performing any action.

       If the captcha is not solved correctly, the window will close terminating the function and the program.

       The function first queries the database to determine if it has to run a full insert of all the records
       (empty database) or if it has to run the update fucntionality as well as the adding of new records indexed.

       Any error, warning encountered during the function operations will be logged in WECPSIE database.

       Examples
       --------
       >>> from selenium import webdriver
       >>> driver = webdriver.Chrome(Driver_PATH)
       >>> swing_index_pages(driver)
       """

    driver.maximize_window()
    driver.get(dutils.SERCOP_URL)

    print(P+"[CRAWLER] Abriendo la pagina de la SERCOP ...")
    print(P+"[CRAWLER] Tiempo para setear los parametros de busqueda...")
    # method to solve captcha here
    #
    #
    #
    time.sleep(1)
    select_type_process = Select(driver.find_element_by_id('txtCodigoTipoCompra'))
    select_type_process.select_by_value("386");

    #The dates could be change
    # FECHA_INICIO = "2020-07-01"
    # FECHA_FINAL = "2020-07-31"

    start_process_date = driver.find_element_by_id('f_inicio')
    driver.execute_script("arguments[0].value = '2020-07-31';", start_process_date)

    end_process_date = driver.find_element_by_id('f_fin')
    driver.execute_script("arguments[0].value = '2020-07-31';", end_process_date)


    #Time to solve Captcha
    time.sleep(12)


    search_button = driver.find_elements_by_id('btnBuscar')

    # validate captcha search
    search_button[1].click()

    db_connection = wpsedb.conect_to_wpsie_db(['localhost', 'wecpsie', 'user', 'pass'])

    new_pcp_count = 0
    updated_pcp_count = 0

    try:

        triad_pks = wpsedb.get_triad_pks('index_search_result', db_connection)
        procesos_indexed = len(triad_pks)

        # insert new records and update existing ones
        if procesos_indexed > 0:
            print(C+"[SWING] {} records found in database, Update Mode".format(
                procesos_indexed))
            print(C+"[SWING] Gathering index search results page information...")

            page_table_info_list, current_last_item, last_item = dextract.get_main_page_table_info(
                driver)
            pcpe_update_list = []
            new_pcpe_list = []

            for pcpe_dict in page_table_info_list:

                extracted_dual_pk = (pcpe_dict['Código'],
                                      pcpe_dict['Entidad Contratante'])

                if extracted_dual_pk in triad_pks:
                    pcpe_update_list.append(pcpe_dict)
                    updated_pcp_count += 1

                else:
                    new_pcpe_list.append(pcpe_dict)
                    new_pcp_count += 1
                pass

            # insert new records
            if len(new_pcpe_list) > 0:
                wpsedb.save_index_search_table(new_pcpe_list, db_connection)
            # update saved records
            if len(pcpe_update_list) > 0:
                wpsedb.save_index_search_table(
                    pcpe_update_list, db_connection, mode="update")

            total_pages = math.floor(last_item/current_last_item)

            next_button = driver.find_element_by_xpath('//*[@id="divProcesos"]/table[2]/tbody/tr[2]/td[3]/a')
            next_button.click()
            time.sleep(1)
            print()

            while current_last_item < last_item:
                try:
                    page_table_info_list, current_last_item, last_item = dextract.get_main_page_table_info(
                        driver)
                    pcpe_update_list = []
                    new_pcpe_list = []

                    for pcpe_dict in page_table_info_list:

                        extracted_dual_pk = (pcpe_dict['Código'],
                                              pcpe_dict['Entidad Contratante'])

                        if extracted_dual_pk in triad_pks:
                            pcpe_update_list.append(pcpe_dict)
                            updated_pcp_count += 1

                        else:
                            new_pcpe_list.append(pcpe_dict)
                            new_pcp_count += 1
                        pass

                    # insert new records
                    if len(new_pcpe_list) > 0:
                        wpsedb.save_index_search_table(
                            new_pcpe_list, db_connection)
                    # update saved records
                    if len(pcpe_update_list) > 0:
                        wpsedb.save_index_search_table(
                            pcpe_update_list, db_connection, mode="update")

                    next_button = driver.find_element_by_xpath('//*[@id="divProcesos"]/table[2]/tbody/tr[2]/td[3]/a')
                    next_button.click()
                    time.sleep(1)

                    # test break
                    if current_last_item == 100:
                        break

                except Exception as e:

                    # LOG area
                    entry_message = "[SWING] WECPSIE failed during the gathering of index information. Last element in page processed: {}\nError message: {}". format(
                        current_last_item, e)
                    print(R+entry_message)

                    wpsedb.add_entry_log(db_connection, '[SWING]', '[SWING]','', 'error', 'navigation','nav',today, entry_message)
                    # test break
                    # if current_last_item == 100:
                    #     break

        # insert all (empty database)
        else:
            print(C+"[SWING] No records found in database, First Run Mode (Insert all)")
            print(C+"[SWING] Gathering index search results page information...")

            page_table_info_list, current_last_item, last_item = dextract.get_main_page_table_info(driver)

            wpsedb.save_index_search_table(page_table_info_list, db_connection)

            total_pages = math.floor(last_item/current_last_item)


            next_button = driver.find_element_by_xpath('//*[@id="divProcesos"]/table[2]/tbody/tr[2]/td[3]/a')
            next_button.click()
            time.sleep(1)
            print()



            while current_last_item < last_item:

                try:
                    page_table_info_list, current_last_item, last_item = dextract.get_main_page_table_info(
                        driver)

                    new_pcp_count += len(page_table_info_list)

                    wpsedb.save_index_search_table(
                        page_table_info_list, db_connection)
                    print()
                    if current_last_item != last_item:
                        next_button = driver.find_element_by_xpath('//*[@id="divProcesos"]/table[2]/tbody/tr[2]/td[3]/a')
                        next_button.click()
                        time.sleep(1)

                    # test break
                    # if current_last_item == 100:
                    #     break

                except Exception as e:

                    # LOG area
                    entry_message = "[SWING] WECPSIE failed during the gathering of index information. Last element in page processed: {}\nError message: {}". format(
                        current_last_item, e)
                    print(R+entry_message)
                    wpsedb.add_entry_log(db_connection, '[SWING]', '[SWING]','', 'error', 'navigation','nav',today, entry_message)

                    # # test break
                    # if current_last_item > 50:
                    #     break


        time.sleep(3)
        wpsedb.disconnect_db(db_connection)
        # driver.close()
        print(G+"[SWING] WECPSIE retrieved and saved/updated all basic indexing information from {} procesos de contratación SIE in {} pages".format(
            last_item, total_pages))
        print(Y+"[SWING] New PCPSIE added:", new_pcp_count)
        print(Y+"[SWING] Updated PCPSIE:", updated_pcp_count)
        print()

    except Exception as e:
        entry_message = "[SWING] WECPSIE failed during the gathering of search index table information.\nError message: {}". format(
            e)
        print(R+entry_message)

        # driver.quit()
        # LOG zone
        wpsedb.add_entry_log(db_connection, '[SWING]', '[SWING]', '', 'error', 'navigation', 'nav', today,
                             entry_message)

def crawl_pcpsie_page(pcrawler):  # level 1 navigation
    """
    Navigator for the information shown in page: Información Proceso Contratación.

    The function handles the navigation and scrapping for the tables Descripción del Proceso de Contratación and Fechas de Control del Proceso.


    Parameters
    ----------
    pcrawler : webdriver instance to perform crawling operations.

    Notes
    -----
    Every Proceso de Contratación Pública Subasta Inversa Electronica (PCPSIE) published has this information page.

    This function does not depend on any user to make any input, since it retrieves the links for each page
    directly from the database. The links to every PCPSIE page are saved to the database during the swing_index_pages
    function.

    Any error, warning encountered during the function operations will be logged in WECPSIE database.

    Examples
    --------
    >>> from selenium import webdriver
    >>> pcrawler = webdriver.Chrome(Driver_PATH)
    >>> crawl_pcpsie_pages(pcrawler)
    """

    print(C+"\n[CRAWL] Fetching PCPSIE links from database...")
    db_connection = wpsedb.conect_to_wpsie_db(['localhost', 'wecpsie', 'user', 'pass'])

    p_link_list = wpsedb.get_procesos_page_links(db_connection)
    links_in_plist = len(p_link_list)

    triad_pks = wpsedb.get_triad_pks('info_sie', db_connection)


    if links_in_plist > 0:
        # p_index = 1
        print(C+"[CRAWL] SIE Crawler started for {} paginas PCPSIE".format(
            links_in_plist))

        for p_index, proceso_link in enumerate(p_link_list):
            # add one because it starts at 0
            p_index += 1
            print(C+"\n[CRAWL] Crawling process {} of {}".format(
                p_index, links_in_plist))
            print(C+"  |--[CRAWL] Opening page for PID:", proceso_link[0])
            pcrawler.get(proceso_link[1])

            try:

                infotable = WebDriverWait(pcrawler, 5).until(EC.presence_of_element_located((By.ID, 'ladoDer')))
                time.sleep(2)

                # retrieve tables descripcion del proceso + fechas de control
                print(C+"  |--[CRAWL] Gathering information from Descripción del Proceso de Contratación and Fechas de Control")

                sie_dict,list_autoridades, list_comision = dextract.extract_SIE_lv1(infotable)

                sie_triad_pk = (sie_dict['Codigo'],
                                sie_dict['Entidad'])

                sie_mode = 'insert' if sie_triad_pk not in triad_pks else 'update'

                print(G+"  |--[CRAWL] Done extracting SIE information!")
                wpsedb.save_SIE_info(sie_dict, list_autoridades,list_comision, db_connection, mode=sie_mode)

                print(G+"  |--[CRAWL] Closing page for PID:", proceso_link[0])



            except Exception as e:
                entry_message = "[CRAWL] Quit unexpectedly: PID:|{}|\nError message: {}".format(
                    proceso_link[0], e)
                print(R+"  |--{}".format(entry_message))

                # LOG zone
                wpsedb.add_entry_log(db_connection, '[CRAWL1]', '[CRAWL1]','', 'error', 'navigation','crawl',today, entry_message)
                # if p_index == 100:
                #     # pcrawler.quit()
                #     break

        print(G+"[CRAWL] Crawled and extracted base level of information in PCPSIE, check log for possible errors.")
        wpsedb.disconnect_db(db_connection)
        #pcrawler.close()

    else:
        print(P+"[CRAWL] There are not any processes stored in WECPSIE_DB index_table")
        # pcrawler.close()
        wpsedb.disconnect_db(db_connection)

def crawl_pcpsie_contrato_page(pcrawler):  # level 2 navigation
    """
    Navigator for the information shown in page: Resumen Información Esencial del Contrato.

    The function handles the navigation and scrapping for the table Información de Adjudicación.


    Parameters
    ----------
    pcrawler : webdriver instance to perform crawling operations.

    Notes
    -----
    Not all the Proceso de Contratación Pública Subasta Inversa Electronica (PCPSIE) published has this information page, it depens of the state of the process

    This function does not depend on any user to make any input, since it retrieves the links for each page
    directly from the database. The links to every PCPSIE page are saved to the database during the crawl_pcpsie_page
    function.

    Any error, warning encountered during the function operations will be logged in WECPSIE database.

    Examples
    --------
    >>> from selenium import webdriver
    >>> pcrawler = webdriver.Chrome(Driver_PATH)
    >>> crawl_pcpsie_contrato_page(pcrawler)
    """


    print(C+"\n[CRAWL] Fetching PCPSIE_contrato links from database...")
    db_connection = wpsedb.conect_to_wpsie_db(['localhost', 'wecpsie', 'user', 'pass'])

    c_link_list = wpsedb.get_contrato_links(db_connection)
    links_in_clist = len(c_link_list)

    prev_pks = wpsedb.get_triad_pks('info_contrato',db_connection)

    counter = 0
    if links_in_clist > 0:
        # c_index = 1
        print(C+"[CRAWL] SIE Crawler started for paginas PCPSIE con contrato disponible")

        for c_index, contrato_link in enumerate(c_link_list):
            # add one because it starts at 0


            if contrato_link[2] != '':
                counter += 1
                c_index += 1
                print(C + "\n[CRAWL] Crawling process {}".format(
                    counter))

                print(C + "  |--[CRAWL] Opening page for PID:", contrato_link[0])
                pcrawler.get(contrato_link[2])

                try:

                    infotable = WebDriverWait(pcrawler, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="frminfoGeneralContrato"]/table')))
                    time.sleep(2)

                    # retrieve information in table Informacion de Adjudicacion
                    print(C+"  |--[CRAWL] Gathering information from Informacion de Adjudicacion")

                    contrato_dic = dextract.extract_SIE_lv2(infotable)

                    sie_triad_pk = (contrato_link[0],contrato_link[1])

                    sie_mode = 'insert' if sie_triad_pk not in prev_pks else 'update'

                    print(G+"  |--[CRAWL] Done extracting contract SIE information!")

                    wpsedb.save_contract_SIE_info(contrato_link[0],contrato_link[1],contrato_dic, db_connection, mode=sie_mode)

                    print(G+"  |--[CRAWL] Closing page for PID:", contrato_link[0])

                    if c_index == 100:
                        pcrawler.close()
                        break

                except Exception as e:
                    entry_message = "[CRAWL] Quit unexpectedly: PID:|{}|\nError message: {}".format(
                        contrato_link[0], e)
                    print(R+"  |--{}".format(entry_message))

                    # LOG zone
                    wpsedb.add_entry_log(db_connection, '[CRAWL2]', '[CRAWL2]', '', 'error', 'navigation', 'crawl',
                                         today, entry_message)
                    # if c_index == 100:
                    #     # pcrawler.quit()
                    #     break

        print(G+"[CRAWL] Crawled and extracted base level  2 of information in PCPSIE, check log for possible errors.")
        wpsedb.disconnect_db(db_connection)
        pcrawler.close()

    else:
        print(P+"[CRAWL] There are not any processes stored in WECPSIE_DB info_sie")
        # pcrawler.close()
        wpsedb.disconnect_db(db_connection)

def crawl_pcpsie_invitacion_page(pcrawler):  # level 2 navigation
    """
    Navigator for the information shown in page: Invitaciones a proveedores.

    The function handles the navigation and scrapping for the table Invitaciones a proveedores.


    Parameters
    ----------
    pcrawler : webdriver instance to perform crawling operations.

    Notes
    -----
    all the Proceso de Contratación Pública Subasta Inversa Electronica (PCPSIE) published has this information page, but
    in some cases there are no invited companies

    This function does not depend on any user to make any input, since it retrieves the links for each page
    directly from the database. The links to every PCPSIE page are saved to the database during the crawl_pcpsie_page
    function.

    Any error, warning encountered during the function operations will be logged in WECPSIE database.

    Examples
    --------
    >>> from selenium import webdriver
    >>> pcrawler = webdriver.Chrome(Driver_PATH)
    >>> crawl_pcpsie_invitacion_page(pcrawler)
    """


    print(C+"\n[CRAWL] Fetching PCPSIE_invitaciones links from database...")
    db_connection = wpsedb.conect_to_wpsie_db(['localhost', 'wecpsie', 'user', 'pass'])

    iv_link_list = wpsedb.get_invitacion_links(db_connection)
    links_in_ivlist = len(iv_link_list)

    prev_pks = wpsedb.get_triad_pks('invitaciones',db_connection)

    counter = 0
    if links_in_ivlist > 0:
        # iv_index = 1
        print(C+"[CRAWL] SIE Crawler started for paginas PCPSIE con lista de invitados disponible")

        for iv_index, invitacion_link in enumerate(iv_link_list):
            # add one because it starts at 0
            if invitacion_link[2] != '':
                counter += 1
                iv_index += 1
                print(C + "\n[CRAWL] Crawling process {}".format(
                    counter))

                print(C + "  |--[CRAWL] Opening page for PID:", invitacion_link[0])
                pcrawler.get(invitacion_link[2])

                try:

                    infotable = WebDriverWait(pcrawler, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="formPaginas"]/table')))
                    time.sleep(1)

                    # retrieve information in table Invitacion a proveedores
                    print(C+"  |--[CRAWL] Gathering information from Invitacion a proveedores")

                    invitacion_list = dextract.extract_SIE_lv2_inv(infotable,pcrawler)

                    sie_triad_pk = (invitacion_link[0],invitacion_link[1])

                    sie_mode = 'insert' if sie_triad_pk not in prev_pks else 'update'

                    print(G+"  |--[CRAWL] Done extracting invitacion SIE information!")

                    wpsedb.save_invitation_SIE_info(invitacion_link[0],invitacion_link[1],invitacion_list, db_connection, mode=sie_mode)

                    print(G+"  |--[CRAWL] Closing process for PID:", invitacion_link[0])

                    if iv_index == 100:
                        pcrawler.close()
                        break

                except Exception as e:
                    entry_message = "[CRAWL] Quit unexpectedly: PID:|{}|\nError message: {}".format(
                        invitacion_link[0], 'No existen invitaciones disponibles')
                    print(R+"  |--{}".format(entry_message))

                    # LOG zone
                    wpsedb.add_entry_log(db_connection, '[CRAWL2]', '[CRAWL2]', '', 'error', 'navigation', 'crawl',
                                         today, entry_message)
                    # if c_index == 100:
                    #     # pcrawler.quit()
                    #     break

        print(G+"[CRAWL] Crawled and extracted base level  2 of information in PCPSIE, check log for possible errors.")
        wpsedb.disconnect_db(db_connection)
        pcrawler.close()

    else:
        print(P+"[CRAWL] There are not any processes stored in WECPSIE_DB info_sie")
        # pcrawler.close()
        wpsedb.disconnect_db(db_connection)



# main program

if __name__ == '__main__':

    start_time = time.time()
    print("\n|----------------------------------------<<WECPSIE - WEB CRAWLER>>-----------------------------------")

    driver = webdriver.Chrome(dutils.GDriver_PATH)
    # swing_index_pages(driver)
    # crawl_pcpsie_page(driver)
    # crawl_pcpsie_contrato_page(driver)
    crawl_pcpsie_invitacion_page(driver)