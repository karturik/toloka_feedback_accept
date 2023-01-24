import pandas as pd
import requests
import toloka.client as toloka
import os
from geopy.geocoders import Nominatim
from functools import partial
import time
import psycopg2

geolocator = Nominatim(user_agent="geoapiExercises")
geocode = partial(geolocator.geocode, language="en")

conn = psycopg2.connect("""
    host=host
    port=port
    sslmode=require
    dbname=dbname
    user=user
    password=password
    target_session_attrs=read-write
""")

encrypt_alpahbet = {}
q = conn.cursor()
q.execute('''SELECT assignment_id, worker_id, assignment_nation, assignment_month,
                                              assignment_send_date, assignment_toloka_date, toloka_status,
                                              reward, account, pool_type, decision, reject_reason,
                                              hashes, gender FROM public.sets ''')
all_sets_in_db_df = pd.DataFrame(q.fetchall(), columns = ['assignment_id', 'worker_id', 'assignment_nation', 'assignment_month',
                                                          'assignment_send_date', 'assignment_toloka_date', 'toloka_status', 'reward',
                                                          'account', 'pool_type', 'decision', 'reject_reason', 'hashes', 'gender'])


URL_WORKER = 'https://toloka.yandex.ru/requester/worker/'
URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

account = ''
skill_id_reject = 1
skill_id_accept = 2

working_excel = pd.read_excel('work.xlsx', sheet_name='Лист1')

pool_number1 = 0

with open('need_manual.tsv', 'w', encoding='utf-8') as file:
    file.write(f"assignment_id\tMust be filled\n")
    file.close()

with open('errors.tsv', 'w', encoding='utf-8') as file:
    file.write(f"assignment_id\terror\n")
    file.close()

# START WORK-EXCEL PROCESSING
for assignment_link in working_excel['assignment_link'].dropna():

    day = working_excel[working_excel['assignment_link']==assignment_link]['date'].values[0]
    if not '-' in str(day):
        day = int(day)
    else:
        day = str(day)
    nation_for_data_base = working_excel[working_excel['assignment_link']==assignment_link]['nation'].values[0]
    if nation_for_data_base == 'Middle East':
        nation_for_data_base = 'Arabians'
    month = working_excel[working_excel['assignment_link']==assignment_link]['month'].values[0]
    bonus_already_were_gived = False
    print(nation_for_data_base, month, day)
    if "@" in assignment_link:
        assignment_link = assignment_link.replace('@', '')
        a_add = True
    else:
        a_add = False
    # DETECT SET FROM TOLOKA AND IT'S PARAMETERS
    if '--' in assignment_link:
        tries = 0
        while tries < 10:
            try:
                if not 'https://' in assignment_link and '--' in assignment_link:
                    assignment_id = assignment_link
                    pool_number = toloka_client.get_assignment(assignment_id=assignment_id).pool_id
                    project_id = toloka_client.get_pool(pool_id=pool_number).project_id
                    assignment_link = f'https://platform.toloka.ai/requester/project/{project_id}/pool/{pool_number}/assignments/{assignment_id}?direction=ASC'
                    print(assignment_link)
                    project_number = assignment_link.split('project/')[1].split('/pool')[0]
                    pool_number = assignment_link.split('/pool/')[1].split('/assignments')[0]
                    assignment_id = assignment_link.split('assignments/')[1].split('?direction')[0]
                    assignment_link = assignment_id
                else:
                    print(assignment_link)
                    project_number = assignment_link.split('project/')[1].split('/pool')[0]
                    pool_number = assignment_link.split('/pool/')[1].split('/assignments')[0]
                    assignment_id = assignment_link.split('assignments/')[1].split('?direction')[0]
                if assignment_id in all_sets_in_db_df['assignment_id'].unique():
                    is_in = True
                relatives_project_numbers = {'123072': '128601', '123536': '128530', '123537': '128510','123538': '128502'}
                relatives_project_number = relatives_project_numbers[project_number]
                relatives_link = f'https://toloka.yandex.ru/tasks?projectId={relatives_project_number}'
                pool_name = toloka_client.get_pool(pool_id=pool_number).private_name
                if 'new' in pool_name.lower() and not 'retry' in pool_name.lower() and not 'родствен' in pool_name.lower():
                    pool_type = 'new'
                elif 'retry' in pool_name.lower():
                    pool_type = 'retry'
                elif 'родствен' in pool_name.lower():
                    pool_type = 'родственники'
                else:
                    pool_type = ''
                print('project_number: ', project_number)
                print('pool_number: ', pool_number)
                print('assignment_id: ', assignment_id)
                assignment_request = toloka_client.get_assignment(assignment_id=assignment_id)
                if pool_number != pool_number1:
                    df_toloka = toloka_client.get_assignments_df(pool_number, status=['APPROVED', 'SUBMITTED', 'REJECTED'])
                    pool_number1 = pool_number
                else:
                    df_toloka = df_toloka

                if 'OUTPUT:race' in df_toloka:
                    try:
                        ethnicity = \
                        df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['OUTPUT:race'].values[0]
                        if ethnicity == 'Middle Eastern':
                            ethnicity = 'Middle East'
                    except Exception as e:
                        with open('need_manual.tsv', 'a', encoding='utf-8') as file:
                            file.write(f"{assignment_id}\tнациональность\n")
                            file.close()
                        with open('errors.tsv', 'a', encoding='utf-8') as file:
                            file.write(f"{assignment_id}\t{e}\n")
                            file.close()
                        ethnicity = ""
                else:
                    with open('need_manual.tsv', 'a', encoding='utf-8') as file:
                        file.write(f"{assignment_id}\tнациональность\n")
                        file.close()
                    ethnicity = ""
                print('ethnicity: ', ethnicity)

                worker_id = \
                df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:worker_id'].values[0]
                worker = requests.get(url='https://toloka.yandex.ru/api/new/requester/workers/' + worker_id,
                                      headers=HEADERS).json()
                print('worker_id: ', worker_id)
                # DEFINITION OF WORKER LANGUAGE BY PROJECT NUMBER
                if project_number == '123538' or project_number == '120426' or project_number == '120106':
                    if 'ES' in worker['languages']:
                        worker_language = 'ES'
                    else:
                        worker_language = 'EN'
                elif project_number == '123536' or project_number == '115606':
                    if 'RU' in worker['languages']:
                        worker_language = 'RU'
                    else:
                        worker_language = 'EN'
                elif project_number == '123537' or project_number == '123072' or project_number == '105897':
                    if 'ES' in worker['languages']:
                        worker_language = 'ES'
                    elif 'FR' in worker['languages']:
                        worker_language = 'FR'
                    elif 'EN' in worker['languages']:
                        worker_language = 'EN'
                    elif 'AR' in worker['languages']:
                        worker_language = 'AR'
                    elif 'ID' in worker['languages']:
                        worker_language = 'ID'
                    else:
                        worker_language = 'EN'
                else:
                    worker_language = 'EN'

                if 'OUTPUT:language' in df_toloka:
                    input_worker_language = \
                    df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['OUTPUT:language'].values[0]
                    if type(input_worker_language) != float:
                        if 'es' in input_worker_language:
                            worker_language = 'ES'
                        elif 'ru' in input_worker_language:
                            worker_language = 'RU'
                        elif 'fr' in input_worker_language:
                            worker_language = 'FR'
                        elif 'tr' in input_worker_language:
                            worker_language = 'TR'
                        elif 'ar' in input_worker_language:
                            worker_language = 'AR'
                        elif 'id' in input_worker_language:
                            worker_language = 'ID'
                else:
                    pass

                print('worker language: ', worker_language)
                # SELECT MESSAGES, TOPICS TEXT BY LANGUAGE
                if worker_language == 'RU':
                    message_text = working_excel['message'][0].replace('{project_number}', project_number)
                    message_text_accept = working_excel['message_accept'][0].replace('{project_number}', project_number)
                    comment = 'Выполнено не по инструкции'
                    topic_reject = "Сделайте 10 фото лица"
                    topic_accept = "Хотите заработать 100$ без усилий?"

                elif worker_language == 'ES':
                    message_text = working_excel['message_espanien'][0].replace('{project_number}', project_number)
                    message_text_accept = working_excel['message_espanien_accept'][0].replace('{project_number}', project_number)
                    comment = 'No cumplida según las instrucciones'
                    topic_reject = "Toma 10 fotos faciales"
                    topic_accept = "¿Quieres ganar 100$ sin esfuerzo?"

                elif worker_language == 'FR':
                    message_text = working_excel['message_francusien'][0].replace('{project_number}', project_number)
                    message_text_accept = working_excel['message_francusien_accept'][0].replace('{project_number}', project_number)
                    comment = 'Non rempli selon les instructions'
                    topic_reject = "Faire 10 photos de visage"
                    topic_accept = "Vous voulez apprendre 100 $sans effort?"

                elif worker_language == 'TR':
                    message_text = working_excel['message_turkey'][0].replace('{project_number}', project_number)
                    message_text_accept = working_excel['message_turkey_accept'][0].replace('{project_number}', project_number)
                    comment = 'Talimatlara göre yapılmadı'
                    topic_reject = "Yüzün 10 fotoğrafını çekin"
                    topic_accept = "Görevi doğru tamamladığınız için çok teşekkür ederim, çabalarınızı gerçekten takdir ediyoruz!"

                elif worker_language == 'AR':
                    message_text = working_excel['message_english'][0].replace('{project_number}', project_number)
                    message_text_accept = working_excel['message_arab_accept'][0].replace('{project_number}', project_number)
                    comment = 'لا تملأ وفقا للتعليمات'
                    topic_reject = "اصنع 10 صور للوجه"
                    topic_accept = "تريد أن تتعلم effortlessly 100 جهد?"

                elif worker_language == 'ID':
                    message_text = working_excel['message_english'][0].replace('{project_number}', project_number)
                    message_text_accept = working_excel['message_indonesian_accept'][0].replace('{project_number}', project_number)
                    comment = 'Tidak dilakukan sesuai dengan instruksi'
                    topic_reject = "Ambil 10 foto wajah"
                    topic_accept = "Terima kasih banyak telah menyelesaikan tugas dengan benar, kami sangat menghargai upaya Anda!"

                else:
                    message_text = working_excel['message_english'][0].replace('{project_number}', project_number)
                    message_text_accept = working_excel['message_english_accept'][0].replace('{project_number}', project_number)
                    comment = 'Not according to the instructions'
                    topic_reject = "Take 10 photos of your face"
                    topic_accept = "Want to earn $100 effortlessly?"

                li_refusal_reason_for_message = message_text.split('<ol>')[1].split('</ol>')[0]
                refusal_reason_for_message = ""
                refusal_reason_text_list = []
                if a_add:
                    refusal_reasons_number_list = working_excel[working_excel['assignment_link'] == '@'+assignment_link]['refusal_reasons'].values[0]
                else:
                    refusal_reasons_number_list = working_excel[working_excel['assignment_link'] == assignment_link]['refusal_reasons'].values[0]
                print(refusal_reasons_number_list)
                if "f" in str(refusal_reasons_number_list).lower():
                    sex = "FEMALE"
                elif "m" in str(refusal_reasons_number_list).lower():
                    sex = "MALE"
                else:
                    sex = False
                refusal_reasons_number_list = str(refusal_reasons_number_list).replace("f", "").replace("m",
                                                                                                        "").replace(
                    " f ", " ").replace(" m ", " ")

                if not "+" in refusal_reasons_number_list and not "-" in refusal_reasons_number_list and not "$" in refusal_reasons_number_list:

##########################  REJECT SET   ##################################
                    if " " in refusal_reasons_number_list.strip():
                        refusal_reasons_number_list = refusal_reasons_number_list.replace("  ", " ").strip().split(" ")
                    else:
                        refusal_reasons_number_list = [int(float(refusal_reasons_number_list.strip()))]
                    print('refusal_reasons_list: ', refusal_reasons_number_list)
                    # SELECT REFUSAL REASONS TEXT BY WORKER LANGUAGE
                    for refusal_reason_number in refusal_reasons_number_list:
                        if worker_language == 'ru':
                            refusal_reason = \
                            working_excel[working_excel['refusal_reasons_number'] == int(refusal_reason_number)][
                                'refusal_reasons_text'].values[0]
                        elif worker_language == 'ES':
                            refusal_reason = \
                            working_excel[working_excel['refusal_reasons_number'] == int(refusal_reason_number)][
                                'refusal_reasons_text_espanien'].values[0]
                        elif worker_language == 'FR':
                            refusal_reason = \
                            working_excel[working_excel['refusal_reasons_number'] == int(refusal_reason_number)][
                                'refusal_reasons_text_francusien'].values[0]
                        elif worker_language == 'TR':
                            refusal_reason = \
                            working_excel[working_excel['refusal_reasons_number'] == int(refusal_reason_number)][
                                'refusal_reasons_text_turkey'].values[0]
                        else:
                            refusal_reason = \
                            working_excel[working_excel['refusal_reasons_number'] == int(refusal_reason_number)][
                                'refusal_reasons_text_english'].values[0]
                        refusal_reason_text_list.append(refusal_reason)
                        refusal_reason_for_message = refusal_reason_for_message + li_refusal_reason_for_message.replace(
                            "#141825;'>", f"#141825;'> {refusal_reason}")
                    message_text = message_text.replace(li_refusal_reason_for_message,
                                                        refusal_reason_for_message).replace('{ACCOUNT}',
                                                                                            'trainingdata.pro').replace('{project_number}', project_number)
                    if df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:status'].values[
                        0] != 'REJECTED' and df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id][
                        'ASSIGNMENT:status'].values[0] != 'APPROVED':
                        toloka_client.reject_assignment(assignment_id=assignment_id, public_comment=comment)
                        print('Отклоняем сет')
                        #REJECT SET IN TOLOKA
                        #SEND MESSAGE
                        message_body = {
                            "topic": {
                                "EN": topic_reject,
                                "RU": topic_reject
                            },
                            "text": {
                                "EN": message_text,
                                "RU": message_text
                            },
                            "recipients_select_type": "DIRECT",
                            "recipients_ids": [worker_id],
                            "answerable": True
                        }

                        requests.post(url='https://toloka.dev/api/v1/message-threads/compose', headers=HEADERS,
                                      data=message_body)

                        url = 'https://toloka.yandex.ru/api/v1/message-threads/compose'
                        send_msg = requests.post(url, headers=HEADERS, json=message_body).json()
                        if 'created' in send_msg:
                            print('Send message')
                        else:
                            print('Cant send message: ', send_msg)
                            with open('need_manual.tsv', 'a', encoding='utf-8') as file:
                                file.write(f"{assignment_id}\tsend message\n")
                                file.close()
                        # CREATE SKILL FOR WORKER
                        skill_body = {
                            "skill_id": skill_id_reject,
                            "user_id": worker_id,
                            "value": 60,
                            "reason": "Rehab 10 photos"
                        }
                        url = 'https://toloka.dev/api/v1/user-skills'
                        add_skill = requests.put(url, headers=HEADERS, json=skill_body).json()
                        if 'created' in add_skill:
                            print('Skill gived')
                        else:
                            print('Skill not gived: ', add_skill)
                            with open('need_manual.tsv', 'a', encoding='utf-8') as file:
                                file.write(f"{assignment_id}\tgive skill\n")
                                file.close()

                    else:
                        print('Set already rejected, its status: ',
                              df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id][
                                  'ASSIGNMENT:status'].values[0])
                    print("-----------------------------------------------")
                    pass

                elif "-" in refusal_reasons_number_list or "-" in refusal_reasons_number_list:
##########################   REJECT SET WITHOUT RETRY   ##################################
                    # REJECT SET WITHOUT WORKER ACCESS TO RETRY POOL
                    if df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:status'].values[0] != 'REJECTED':
                        toloka_client.reject_assignment(assignment_id=assignment_id, public_comment=comment)
                        print('Reject set')
                    else:
                        print('Set already rejected, its status: ', df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:status'].values[0])
                    print("-----------------------------------------------")
                    pass

                elif refusal_reasons_number_list == "$" or "$" in refusal_reasons_number_list:
##########################   ACCEPT SET   ##################################
                    if df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:status'].values[0] != 'APPROVED':
                        encrypted_worker_id = ''
                        for s in worker_id:
                            s = encrypt_alpahbet.get(s)
                            encrypted_worker_id += s

                        toloka_client.accept_assignment(assignment_id=assignment_id, public_comment='Accepted')
                        print('Accept set')
                        skill_body = {
                            "skill_id": skill_id_accept,
                            "user_id": worker_id,
                            "value": 60,
                            "reason": "Согласился прислать 10 фото родственников"
                        }
                        url = 'https://toloka.dev/api/v1/user-skills'
                        add_skill = requests.put(url, headers=HEADERS, json=skill_body).json()
                        if 'created' in add_skill:
                            print('Skill given')
                        else:
                            print('Skill not given: ', add_skill)
                            with open('need_manual.tsv', 'a', encoding='utf-8') as file:
                                file.write(f"{assignment_id}\tgive skill\n")
                                file.close()

                        message_text_accept = message_text_accept.replace('{encrypted_worker_id}', encrypted_worker_id).replace('{relatives_link}', relatives_link)

                        message_body_with_referral_code = {
                            "topic": {
                                "EN": f"{topic_accept}",
                            },

                            "text": {
                                "EN": f"{message_text_accept}",
                            },
                            "recipients_select_type": "DIRECT",
                            "recipients_ids": [worker_id],
                            "answerable": True
                        }

                        url = 'https://toloka.yandex.ru/api/v1/message-threads/compose'
                        message_was_sended = False
                        send_tries = 0
                        while message_was_sended != True:
                            try:
                                send_msg_with_code = requests.post(url, headers=HEADERS, json=message_body_with_referral_code).json()
                                if 'created' in send_msg_with_code:
                                    print('Send message with code')
                                else:
                                    print('Cant send message with code: ', send_msg_with_code)
                                message_was_sended = True
                            except Exception as e:
                                print(e)
                                send_tries += 1
                                if send_tries == 10:
                                    with open('need_manual.tsv', 'a', encoding='utf-8') as file:
                                        file.write(f"{assignment_id}\tsend message\n")
                                        file.close()
                                    message_was_sended = True
                        balance = requests.get('https://toloka.dev/api/v1/requester', headers=HEADERS).json()
                        if balance['balance'] > 90.00:
                            if 'OUTPUT:referral' in df_toloka[df_toloka['ASSIGNMENT:assignment_id']==assignment_id]:
                                referral_code = df_toloka[df_toloka['ASSIGNMENT:assignment_id']==assignment_id]['OUTPUT:referral'].values[0]
                                if type(referral_code) == str:
                                    if len(referral_code) > 7 and not " " in referral_code and not '!' in referral_code and not '.' in referral_code:
                                        try:
                                            dencrypted_worker_id = ''
                                            for s in referral_code:
                                                key = [k for k, v in encrypt_alpahbet.items() if v == s][0]
                                                dencrypted_worker_id += key
                                            worker = requests.get(url='https://toloka.dev/api/v1/user-metadata/' + dencrypted_worker_id,headers=HEADERS).json()
                                            if 'country' in worker:
                                                print(worker)
                                                decision = input(f'{dencrypted_worker_id} - worker from referral code, "+" => give bonus')
                                                if decision == "+":
                                                    try:
                                                        if bonus_already_were_gived == False:
                                                            print('Give nonus to worker: ', dencrypted_worker_id, 'from worker: ', worker_id)
                                                            bonus_body = {
                                                                          "user_id": dencrypted_worker_id,
                                                                          "amount": 2.0,
                                                                          "assignment_id": assignment_id,
                                                                          "private_comment": f"to: {dencrypted_worker_id}, from: {worker_id}, for_assignment: {assignment_id}",
                                                                          "public_title": {
                                                                            "EN": "We get good photos from user with your referral code, thank you!",
                                                                            "RU": "Мы получили отличные фото от пользователя с вашим реферральным кодом, большое спасибо! ",
                                                                            "ES": f'Hemos recibido excelentes fotos del usuario con su código de referencia, ¡muchas gracias!',
                                                                            "PT": f'Recebemos ótimas fotos de um usuário com seu código de referência, muito obrigado!',
                                                                            "FR": f'Nous avons reçu dexcellentes photos de lutilisateur avec votre code de référence, merci beaucoup!'
                                                                          },
                                                                          "public_message": {
                                                                            "EN": "We received good photos from user with your referral code, thank you so much! We have send you your reward!",
                                                                            "RU": 'Мы получили отличные фото от пользователя с вашим реферральным кодом, большое спасибо! Мы перечислили вам вашу награду!',
                                                                            "ES": f'Hemos recibido excelentes fotos del usuario con su código de referencia, ¡muchas gracias! ¡Te hemos enumerado tu recompensa!',
                                                                            "PT": f'Recebemos ótimas fotos de um usuário com seu código de referência, muito obrigado! Nós listamos sua recompensa!',
                                                                            "FR": f'Nous avons reçu dexcellentes photos de lutilisateur avec votre code de référence, merci beaucoup! Nous vous avons énuméré votre récompense!'
                                                                          },
                                                                          "without_message": False
                                                                        }
                                                            give_bonus = requests.post(f'https://toloka.yandex.ru/api/v1/user-bonuses', headers=HEADERS, json = bonus_body).json()
                                                            print(give_bonus)

                                                            print('Give bonus to new worker: ', worker_id)
                                                            bonus_body_for_new_user = {
                                                                          "user_id": worker_id,
                                                                          "amount": 0.5,
                                                                          "assignment_id": assignment_id,
                                                                          "private_comment": f"to: {worker_id}, by_code_from: {dencrypted_worker_id}, for_assignment: {assignment_id}",
                                                                          "public_title": {
                                                                            "EN": "Here is your bonus for photos with referral code! Thank you very much!",
                                                                            "RU": "Вот ваш бонус за фото по реферральному коду! Большое спасибо!",
                                                                            "ES": f'¡Aquí está su bono de foto en el código de referencia! ¡Muchas gracias!',
                                                                            "PT": f'Aqui está o seu bônus de foto com o código de referência! Muito obrigado!',
                                                                            "FR": f'Voici votre bonus de photo par code de référence! Grand merci!'
                                                                          },
                                                                          "public_message": {
                                                                            "EN": "Here is your bonus for photos with referral code! Thank you very much! Remember, that number of invited people is not limited",
                                                                            "RU": "Вот ваш бонус за фото по реферральному коду! Большое спасибо! Помните, что количество приглашенных людей не ограничено.",
                                                                            "ES": f'¡Aquí está su bono de foto en el código de referencia! ¡Muchas gracias! Recuerde que el número de personas invitadas no está limitado.',
                                                                            "PT": f'Aqui está o seu bônus de foto com o código de referência! Muito obrigado! Lembre-se de que o número de pessoas convidadas não é limitado.',
                                                                            "FR": f'Voici votre bonus de photo par code de référence! Grand merci! Rappelez-vous que le nombre de personnes invitées nest pas limité.'
                                                                          },
                                                                          "without_message": False
                                                                        }
                                                            give_bonus_for_new_user = requests.post(f'https://toloka.yandex.ru/api/v1/user-bonuses', headers=HEADERS, json = bonus_body_for_new_user).json()
                                                            print(give_bonus_for_new_user)
                                                            bonus_already_were_gived = True
                                                        else:
                                                            print('Bonus already was given')
                                                    except Exception as e:
                                                        print('Cant find worker: ', dencrypted_worker_id, 'error: ', e)
                                                else:
                                                    print('Cant find worker or rhere is some error, worker id: ', dencrypted_worker_id)
                                        except Exception as e:
                                            print(f'Cant dencrypt {referral_code}, error: ', e)
                        else:
                            print('Low balance: ', balance['balance'])
                    else:
                        print('Set already was accepted: ',df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:status'].values[0])

                    try:
                        if assignment_id in all_sets_in_db_df['assignment_id'].unique():
                            q.execute(f"UPDATE public.sets SET decision = 'ACCEPTED', toloka_status = 'APPROVED', assignment_nation = '{nation_for_data_base}',"
                                      f" assignment_month = '{month}', assignment_send_date = '{day}' WHERE assignment_id = '{assignment_id}';")
                            conn.commit()
                            is_in = True
                            print('Update set in database')
                        else:
                            is_in = False
                        if is_in == False:
                            print(f'No id in database: {assignment_id}')
                            q.execute(f"INSERT INTO public.sets (assignment_id, worker_id, assignment_nation, assignment_toloka_date,"
                                      f" toloka_status, reward, account, pool_type, decision, reject_reason, assignment_month, assignment_send_date)"
                                      f" VALUES ('{assignment_id}', '{worker_id}', '{nation_for_data_base}',"
                                      f" '{df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:started'].values[0].split('T')[0]}',"
                                      f" 'APPROVED', '{toloka_client.get_assignment(assignment_id=assignment_id).reward}',"
                                      f" '{account}', '{pool_type}', 'ACCEPTED', 'None', '{month}', '{day}')")
                            conn.commit()
                            print(f'Insert in database: {assignment_id}')
                            with open('assignments_to_db.tsv', 'a', encoding='utf-8') as file:
                                file.write(assignment_id + '\n')
                                file.close()

                    except Exception as e:
                        print(f'Cant update set in database: {assignment_id}', e)
                    print("-----------------------------------------------")
                    pass

                elif refusal_reasons_number_list == "+" or "+" in refusal_reasons_number_list:
##########################   DOWNLOAD SET   ##################################
                    print('That is feedback process, cant download sets')
                tries = 10
            except Exception as e:
                if 'DoesNotExistApiError' in str(e):
                    print('Change account')
                    if OAUTH_TOKEN == '':
                        OAUTH_TOKEN_2 = ''
                        account = ''
                        skill_id_reject = 1
                        skill_id_accept = 2
                    elif OAUTH_TOKEN == '':
                        OAUTH_TOKEN_1 = ''
                        account = ''
                        skill_id_reject = 3
                        skill_id_accept = 4
                    HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
                    toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')
                else:
                    print(e)
                tries += 1
                print(f'Error, try {tries}/10')
                time.sleep(1)
                if tries == 10:
                    with open('errors.tsv', 'a', encoding='utf-8') as file:
                        file.write(f"{assignment_id}\t{e}\n")
                        file.close()

    else:
        print(assignment_link)
        print('Detected inhouse set')
        assignment_id = assignment_link
        # q.execute('''SELECT assignment_id FROM public.sets ''')
        refusal_reasons_number_list = str(working_excel[working_excel['assignment_link'] == assignment_link]['refusal_reasons'].values[0])

        if refusal_reasons_number_list == "$" or "$" in refusal_reasons_number_list:
            update_query = f"UPDATE public.sets SET decision = 'ACCEPTED', toloka_status = 'APPROVED'," \
                           f" assignment_nation = '{nation_for_data_base}', assignment_month = '{month}'," \
                           f" assignment_send_date = '{day}' WHERE assignment_id = '{assignment_id}';"
            add_query = f"INSERT INTO public.sets (assignment_id, worker_id, assignment_nation, assignment_toloka_date," \
                        f" toloka_status, reward, account, pool_type, decision, reject_reason, assignment_month," \
                        f" assignment_send_date) VALUES ('{assignment_id}', 'None', '{nation_for_data_base}'," \
                        f" 'None', 'None', 'None', 'inhouse', 'inhouse', 'ACCEPTED', 'None', '{month}', '{day}')"
            print('Set accepted')
        else:
            if " " in refusal_reasons_number_list.strip() and not '$' in refusal_reasons_number_list.strip():
                refusal_reasons_number_list = refusal_reasons_number_list.replace("  ", " ").strip().split(" ")
            else:
                refusal_reasons_number_list = [int(float(refusal_reasons_number_list.strip()))]
            print('refusal_reasons_list: ', refusal_reasons_number_list)
            # print('type_refusal_reasons_list: ', type(refusal_reasons_number_list))
            refusal_reason_text_list = []
            for refusal_reason_number in refusal_reasons_number_list:
                refusal_reason = working_excel[working_excel['refusal_reasons_number'] == int(refusal_reason_number)]['refusal_reasons_text_english'].values[0]
                refusal_reason_text_list.append(refusal_reason)
            update_query = f"UPDATE public.sets SET decision = 'REJECTED', toloka_status = 'None', reject_reason = '{';'.join(refusal_reason_text_list)}' WHERE assignment_id = '{assignment_id}';"
            add_query = f"INSERT INTO public.sets (assignment_id, worker_id, assignment_nation, assignment_toloka_date," \
                        f" toloka_status, reward, account, pool_type, decision, reject_reason, assignment_month," \
                        f" assignment_send_date) VALUES ('{assignment_id}', 'None', '{nation_for_data_base}', 'None'," \
                        f" 'None', 'None', 'inhouse', 'inhouse', 'REJECTED', '{';'.join(refusal_reason_text_list)}', '{month}', '{day}')"
        try:
            if assignment_id in all_sets_in_db_df['assignment_id'].unique():
                q.execute(update_query)
                print('Set updated in database')
                conn.commit()
                is_in = True
            else:
                is_in = False
            if is_in == False:
                print(f'No set in database: {assignment_id}')
                q.execute(add_query)
                conn.commit()
                print(f'Insert in database: {assignment_id}')
                with open('assignments_to_db.tsv', 'a', encoding='utf-8') as file:
                    file.write(assignment_id + '\n')
                    file.close()
        except Exception as e:
            print(f'Cant update in database: {assignment_id}', e)
        print("-----------------------------------------------")

conn.close()

if os.path.exists('need_manual.tsv'):
    manual_df = pd.read_csv('need_manual.tsv', sep='\t')
    manual_count = len(manual_df['assignment_id'])
else:
    manual_count = 0

if os.path.exists('errors.tsv'):
    error_df = pd.read_csv('errors.tsv', sep='\t')
    error_count = len(error_df['assignment_id'])
else:
    error_count = 0

print(f'Need insert manual: {manual_count}, file: need_manual.tsv')
print(f'Error count: {error_count}, file: errors.tsv')