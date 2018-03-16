# coding=utf-8
"""
InstaPolice Bot Docs.

Этот бот умеет собирать информацию по постам Instagram,
которые размещенают участники группового чата в Telegram,
и выдавать по запросу статистику, кто из участников чата прокомментировал посты в
Instagram.
"""

import sys
import re
import psycopg2
import datetime
import coloredlogs
from itertools import groupby
from telegram.ext import (Updater, CommandHandler, CallbackQueryHandler,
                         ConversationHandler, RegexHandler, Handler,
                         MessageHandler, Filters, StringCommandHandler)
from telegram import (ReplyKeyboardMarkup, ReplyKeyboardRemove,
                     InlineKeyboardButton, InlineKeyboardMarkup, ParseMode)
from telegram.error import (TelegramError, Unauthorized, BadRequest,
                            TimedOut, ChatMigrated, NetworkError)
from termcolor import colored
from collections import OrderedDict
import colorama
colorama.init()

sys.path.append('***')
from logster import init_logger, log_every_sneeze
init_logger(__name__)
import instagram


TOKEN = '***'

INSTAGENT = instagram.Agent()

CONN = psycopg2.connect("***")
CUR = CONN.cursor()

SELECT_CHAT, SET_COMMENTATOR, MENU = range(3)

__LOCAL_CACHE__groups = []


# @log_every_sneeze
def error(bot, update, error):
    raise Exception('Update "{}" caused error "{}"'.format(
        update, error
    ))

@log_every_sneeze
def _save_commentator(insta_name, user):
    command = u"""
        UPDATE telegramer
        SET insta_commentator = '{insta_name}'
        WHERE telegram_id = '{telegram_id}'
    """
    values = {'insta_name': insta_name, 'telegram_id': user.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()

@log_every_sneeze
def set_instagram_commentator(bot, update):
    data = update.message.text
    insta_name = data.lower().strip('@').strip(' ')

    _save_commentator(insta_name, update.effective_user)

    update.message.reply_text("Никнейм вашего профиля в Instagram сохранен!")
    start(bot, update)


def __get_any_user_name(user):
    return (user.username or
            (user.first_name + ' ' + (user.last_name or '')).strip() or
            'NoNamie')


@log_every_sneeze
def _update_telegramer_in_db(user):
    command = u"""
        INSERT INTO telegramer (name, telegram_id)
        VALUES ('{name}', '{telegram_id}')
        ON CONFLICT (telegram_id) DO UPDATE
        SET name=excluded.name
    """
    values = {'name': __get_any_user_name(user), 'telegram_id': user.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()


@log_every_sneeze
def _link_telegramer_to_group(user, chat):
    command = u"""
        INSERT INTO telegramer_groupchat (telegramer_id, groupchat_id)
        SELECT t.id, g.id
        FROM telegramer t, groupchat g
        WHERE t.telegram_id = '{user_id}'
        AND g.telegram_id = '{chat_id}'
        ON CONFLICT ON CONSTRAINT id DO NOTHING
    """
    values = {'user_id': user.id, 'chat_id': chat.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()


@log_every_sneeze
def _save_group(chat):
    command = u"""
        INSERT INTO groupchat (name, telegram_id)
        VALUES ('{name}', '{telegram_id}')
        ON CONFLICT (telegram_id) DO UPDATE
        SET name=excluded.name
    """
    values = {'name': chat.title, 'telegram_id': chat.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()


@log_every_sneeze
def register(bot, update):
    if not update.effective_chat.id in __LOCAL_CACHE__groups:
        _save_group(update.effective_chat)
        __LOCAL_CACHE__groups.append(update.effective_chat.id)
    _update_telegramer_in_db(update.effective_user)
    _link_telegramer_to_group(update.effective_user, update.effective_chat)
    update.message.reply_text(
        "Спасибо, {}! Теперь тебе доступна актуальная статистика по чату \"{}\"".format(
        __get_any_user_name(update.effective_user),
        update.effective_chat.title
        )
    )


@log_every_sneeze
def save_new_group_member(bot, update):
    for user in update.message.new_chat_members:
        if user.is_bot:
            if not user.bot.id == bot.id:
                return
            _save_group(update.message.chat)
        else:
            _update_telegramer_in_db(user)
            _link_telegramer_to_group(user, update.message.chat)


@log_every_sneeze
def _get_telegramer(user):
    command = u"""
        SELECT id, name, insta_commentator
        FROM telegramer
        WHERE telegram_id = '{telegram_id}'
    """
    values = {'telegram_id': user.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    res = CUR.fetchone()
    return res or (None, '', '')

@log_every_sneeze
def _get_groupchat(chat):
    command = u"""
        SELECT id, name
        FROM groupchat
        WHERE telegram_id = '{telegram_id}'
    """
    values = {'telegram_id': chat.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    res = CUR.fetchone()
    return res or (None, '')


@log_every_sneeze
def _get_chats_for_user(user):
    command = u"""
        SELECT id, name
        FROM groupchat
        WHERE id IN(
            SELECT groupchat_id
            FROM telegramer_groupchat
            WHERE telegramer_id = (
                SELECT id
                FROM telegramer
                WHERE telegram_id = '{telegram_id}'
            )
        )
    """
    values = {'telegram_id': user.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    res = CUR.fetchall()
    return res or [(None, '')]


@log_every_sneeze
def _show_chat_choice_or_menu(bot, update):
    chats = _get_chats_for_user(update.effective_user)
    if len(chats) > 1:
        request = (u"\U0001F334 Выберите чат \U0001F334")
        keyboard = []
        for chat in chats:
            chatid, chatname = chat
            btn = [InlineKeyboardButton(chatname,
                                        callback_data='chat#{}'.format(chatid)
                                        )]
            keyboard.append(btn)
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.callback_query:
            query = update.callback_query
            bot.edit_message_text(text=request,
                                  chat_id=query.message.chat_id,
                                  message_id=query.message.message_id,
                                  reply_markup=reply_markup
                                  )
        else:
            update.message.reply_text(request, reply_markup=reply_markup)
        return SELECT_CHAT
    else:
        _show_menu(bot, update, show_back=False)
        return MENU


@log_every_sneeze
def start(bot, update):
    user_id, user_name, name_for_comments = _get_telegramer(update.effective_user)

    if not user_id:
        greeting = (u"Приветствую! Вы пока не зарегистрированы в боте. Для регистрации " +
                     u"вернитесь в групповой чат, и отправьте команду /register" +
                     u"\nЖду обратно!!! \U0001F335")

        update.message.reply_text(greeting, reply_markup=ReplyKeyboardRemove())
        return

    if user_id and not user_name == update.effective_user.username:
        greeting = (u"Приветствую! У вас новый ник в телеграме? Для его регистрации " +
                     u"вернитесь в групповой чат, и отправьте команду /register" +
                     u"\nЖду обратно!!! \U0001F335")

        update.message.reply_text(greeting, reply_markup=ReplyKeyboardRemove())
        return

    if not name_for_comments:
        request = (u"Чтобы отслеживать ваши комментарии к постам в инстаграмме, " +
                    u"мне нужен юзернейм аккаунта, с которого осуществляется " +
                    u"комментирование. Отправьте его \U0001F34C \U0001F49C")
        update.message.reply_text(request, reply_markup=ReplyKeyboardRemove())
        return SET_COMMENTATOR

    return _show_chat_choice_or_menu(bot, update)


@log_every_sneeze
def _save_chosen_chat(chat_id, user_telegram_id):
    command = u"""
        UPDATE telegramer
        SET last_chosen_chat = '{chat_id}'
        WHERE telegram_id = '{telegram_id}'
    """
    values = {'chat_id': chat_id, 'telegram_id': user_telegram_id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()


@log_every_sneeze
def _get_current_chat(user):
    command = u"""
        SELECT id, name
        FROM groupchat
        WHERE id IN(
            SELECT last_chosen_chat
            FROM telegramer
            WHERE telegram_id = '{telegram_id}'
        )
    """
    values = {'telegram_id': user.id}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    res = CUR.fetchone()
    # No record about chosen chat - the user has only one chat
    if not res:
        chatid, chatname = _get_chats_for_user(user)[0]
        if chatid and user:
            _save_chosen_chat(chatid, user.id)
            return (chatid, chatname)
    return res or (None, '')


@log_every_sneeze
def _show_menu(bot, update, show_back=False):
    keyboard = [[InlineKeyboardButton('Статистика', callback_data='stats')]
           ]
    if show_back:
        back_btn = [InlineKeyboardButton('<< Назад к выбору чатов', callback_data='back')]
        keyboard.append(back_btn)

    reply_markup = InlineKeyboardMarkup(keyboard)
    id, current_user_chat = _get_current_chat(update.effective_user)

    reply = u"\u2728 Чат {} \u2728".format(current_user_chat)
    if update.callback_query:
        query = update.callback_query
        bot.edit_message_text(text=reply,
                              chat_id=query.message.chat_id,
                              message_id=query.message.message_id,
                              reply_markup=reply_markup
                              )
    else:
        update.message.reply_text(reply, reply_markup=reply_markup)


@log_every_sneeze
def handle_menu_choice(bot, update):
    data = update.callback_query.data

    if data == 'back':
        return _show_chat_choice_or_menu(bot, update)
    if data == 'stats':
        _show_statistics(bot, update)


@log_every_sneeze
def save_chosen_chat(bot, update):
    data = update.callback_query.data

    if not 'chat#' in data:
        return _show_chat_choice_or_menu(bot, update)
    chat_id = data.split('chat#')[-1]

    user_telegram_id = update.effective_user.id

    _save_chosen_chat(chat_id, user_telegram_id)

    _show_menu(bot, update, show_back=True)

    return MENU


@log_every_sneeze
def _show_statistics(bot, update):
    current_group = _get_current_chat(update.effective_user)[0]

    # Refresh comments firstly
    command = u"""
        SELECT id, insta_link
        FROM post
        WHERE chatted_at >= CURRENT_DATE - 1
        AND groupchat = '{groupchat}'
    """
    values = {'groupchat': current_group}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    res = CUR.fetchall()

    for post_id, link in res:
        post_media = instagram.Media(link)
        comments = INSTAGENT.getComments(post_media, count=500)
        for comment in comments:
            _save_comment(comment, post_id)


    # Then collect statistics
    command = u"""
        SELECT DISTINCT p.id, p.insta_link, p.telegramer, p.author, pa.name, t.name, t.insta_commentator
        FROM post p, telegramer t, telegramer pa
        WHERE p.chatted_at >= CURRENT_DATE - 1
        AND p.groupchat = '{groupchat}'
        AND pa.id = p.telegramer
        AND t.id NOT IN (SELECT telegramer FROM comment WHERE post = p.id)
        AND t.id != pa.id
    """
    values = {'groupchat': current_group}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    no_comments = set(CUR.fetchall())

    command = u"""
        SELECT DISTINCT p.id, p.insta_link, p.telegramer, p.author, pa.name, t.name, t.insta_commentator
        FROM post p, telegramer t, telegramer pa
        WHERE p.chatted_at >= CURRENT_DATE - 1
        AND p.groupchat = '{groupchat}'
        AND pa.id = p.telegramer
        AND NOT (t.id IN (SELECT c.telegramer FROM comment c WHERE c.morethan4 = TRUE AND p.id = c.post))
        AND t.id != pa.id
    """
    values = {'groupchat': current_group}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    lessthan4 = set(CUR.fetchall())

    lessthan4 = lessthan4.difference(no_comments)

    post_stats = OrderedDict()

    for post_id, group in groupby(no_comments, lambda x: x[0]):
        post_stats[post_id] = {u'НЕ ОТКОММЕНТИЛИ': list(group)}

    for post_id, group in groupby(lessthan4, lambda x: x[0]):
        post_stats.setdefault(post_id, {}).update({u'МЕНЬШЕ 4 СЛОВ': list(group)})

    query = update.callback_query
    if not post_stats.items():
        reply = "Нет актуальных постов!"
        bot.send_message(chat_id=query.message.chat_id, text=reply,
                         parse_mode=ParseMode.MARKDOWN)
        return

    bigfatreply = (u"Ниже список постов за ВЧЕРА и СЕГОДНЯ и под каждым - список тех участников" +
                  u" чата, комментарии которых НЕ УДОВЛЕТВОРЯЮТ УСЛОВИЯМ ЧАТА.")

    for post_id, bad_bunches in post_stats.items():
        post_header = (u"\n\n \U0001F308 https://www.instagram.com/p/{link}/\n*АВТОР*: @[{telegram}]" +
                      u" (Instagram: [{instagram}](https://www.instagram.com/{instagram}/))\n")

        example = next(r[0] for r in (bad_bunches.values()) if r)
        post_header_dct = {
            'link': example[1],
            'telegram': example[4],
            'instagram': example[3]
        }

        msg_body = post_header.format(**post_header_dct)
        for reason, records in bad_bunches.items():
            msg_body += "\n" + reason + ":\n"
            for record in records:
                naughty_list_item = "{icon} @[{telegram}] (Instagram: [{instagram}](https://www.instagram.com/{instagram}/))\n"
                naughty_list_item_dct = {
                    'icon': (u'\U0001F525' if record[-1] else u'\U0001F47E'),
                    'telegram': record[-2],
                    'instagram': record[-1] or '???'
                }
                msg_body += naughty_list_item.format(**naughty_list_item_dct)
        bigfatreply += msg_body

    bot.send_message(chat_id=query.message.chat_id, text=bigfatreply,
                     parse_mode=ParseMode.MARKDOWN)


@log_every_sneeze
def quit(bot, update):
    pass

@log_every_sneeze
def _get_telegramer_for_commentator(insta_username):
    command = u"""
        SELECT id, name
        FROM telegramer
        WHERE insta_commentator = '{insta_username}'
    """
    values = {'insta_username': insta_username}
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()
    res = CUR.fetchone()
    return res or (None, '')


@log_every_sneeze
def _save_post(update, media, chatted_at):
    telegramer_id = _get_telegramer(update.effective_user)[0]
    groupchat_id = _get_groupchat(update.effective_chat)[0]
    if not telegramer_id or not groupchat_id:
        return
    command = u"""
        INSERT INTO post (insta_id, author, insta_link, date, chatted_at, telegramer, groupchat)
        VALUES ('{insta_id}', '{author}', '{insta_link}', '{date}', '{chatted_at}', '{telegramer}','{groupchat}')
        ON CONFLICT (insta_id) DO UPDATE
          SET author = excluded.author,
              date = excluded.date;
    """
    values = {
    'insta_id': media.id,
    'author': media.owner,
    'insta_link': media.code,
    'date': datetime.datetime.fromtimestamp(media.date),
    'chatted_at': chatted_at,
    'telegramer': telegramer_id,
    'groupchat': groupchat_id
    }
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()


@log_every_sneeze
def _save_comment(comment, post_id):
    telegramer_id = _get_telegramer_for_commentator(comment.owner)[0]
    if not telegramer_id:
        return
    command = u"""
        INSERT INTO comment (insta_id, author, date, text, post, telegramer, morethan4)
        VALUES ('{insta_id}', '{author}', '{date}', '{text}', '{post}', '{telegramer}', '{morethan4}')
        ON CONFLICT (insta_id) DO UPDATE
          SET author = excluded.author,
              date = excluded.date,
              text = excluded.text,
              morethan4 = excluded.morethan4
              ;
    """
    values = {
    'insta_id': comment.id,
    'author': comment.owner,
    'date': datetime.datetime.fromtimestamp(comment.data),
    'text': comment.text,
    'post': post_id,
    'telegramer': telegramer_id,
    'morethan4': len(re.findall(r'\w+', comment.text)) >= 4
    }
    expression = command.format(**values)
    CUR.execute(expression)
    CONN.commit()


@log_every_sneeze
def process_update(bot, update):
    if not isinstance(update.message.text, str):
        return
    instalink = r"https://[www.]*instagram\.com/p/([^\s/]+)"
    result = re.findall(instalink, update.message.text)
    for post_code in result:
        media = instagram.Media(post_code)
        INSTAGENT.update(media)
        _save_post(update, media, update.message.date)
        # ====== get db ID of the post ============
        command = u"""
            SELECT id
            FROM post
            WHERE insta_id = '{post_insta_id}'
        """
        values = {'post_insta_id': media.id}
        expression = command.format(**values)
        CUR.execute(expression)
        CONN.commit()
        post_id = CUR.fetchone()[0]
        # =========================================

        comments = INSTAGENT.getComments(media, count=100)
        for comment in comments:
            _save_comment(comment, post_id)


@log_every_sneeze
def change_commentator(bot, update, *args, **kwargs):
    data = kwargs.get('args',)
    if not data:
        return
    insta_name = data[0].lower().strip('@').strip(' ')

    _save_commentator(insta_name, update.effective_user)

    update.message.reply_text("Никнейм вашего профиля в Instagram изменен на {}!".format(
                               insta_name
    ))


@log_every_sneeze
def help_info(bot, update):
    help_text = '''
*Начало работы*
1. В групповом чате отправьте команду /register
   для привязки к данному чату.
   Так бот сможет отслеживать Ваши посты.
2. В приватном чате с ботом отправьте команду /start.
   После этого бот предложит ввести ник в Instagram,
   с которого вы комментируете посты других
   участников чата.

*Далее работа осуществляется в приватном чате*:
1. /start - Для выбора чата (если вы состоите в
   нескольких) и получения статистики по чату.
   Статистика выводится по постам, размещенным
   за вчерашний и сегодняшний день.
2. /commentator - Сменить/исправить свое ник,
   использующийся для комментирования в Instagram.
    '''
    update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)



@log_every_sneeze
def main():
    UPDATER =  Updater(TOKEN)
    DISPATCHER = UPDATER.dispatcher

    start_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start, filters=Filters.private)],
        states={
            SELECT_CHAT: [CallbackQueryHandler(save_chosen_chat)],
            SET_COMMENTATOR: [MessageHandler(Filters.text,
                                           set_instagram_commentator, allow_edited=True)
                               ],
            MENU: [CallbackQueryHandler(handle_menu_choice)]
        },
        fallbacks=[CommandHandler('start', start, filters=Filters.private),
                   CommandHandler('commentator', change_commentator, filters=Filters.private, pass_args=True)],
        per_chat=True
    )

    DISPATCHER.add_handler(MessageHandler(
                                    (Filters.status_update.new_chat_members & Filters.group),
                                    save_new_group_member))
    DISPATCHER.add_handler(CommandHandler('register', register, filters=Filters.group))
    DISPATCHER.add_handler(CommandHandler('help', help_info))

    DISPATCHER.add_handler(MessageHandler(Filters.group, process_update))


    DISPATCHER.add_handler(start_handler)

    DISPATCHER.add_error_handler(error)

    UPDATER.start_polling()

    UPDATER.idle()

if __name__ == '__main__':
    main()
