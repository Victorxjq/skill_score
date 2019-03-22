# def skill_words_clean(word):
#     import re
#     try:
#         # Wide UCS-4 build
#         myre = re.compile(u'['
#                           u'\U0001F300-\U0001F64F'
#                           u'\U0001F680-\U0001F6FF'
#                           u'\u2600-\u2B55'
#                           u'\u23cf'
#                           u'\u23e9'
#                           u'\u231a'
#                           u'\u3030'
#                           u'\ufe0f'
#                           u"\U0001F600-\U0001F64F"  # emoticons
#                            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
#                             u'\U00010000-\U0010ffff'
#                            u'\U0001F1E0-\U0001F1FF'  # flags (iOS)
#                            u'\U00002702-\U000027B0'
#                           u'ℂ]+',
#                           re.UNICODE)
#     except re.error:
#         # Narrow UCS-2 build
#         # print('ee')
#         myre =   re.compile(u'('
#                                   u'\ud83c[\udf00-\udfff]|'
#                                   u'\ud83d[\udc00-\ude4f]|'
#                                   u'\uD83D[\uDE80-\uDEFF]|'
#                                   u"(\ud83d[\ude00-\ude4f])|"  # emoticon
#                                   u'[\u2600-\u2B55]|'
#                                   u'[\u23cf]|'
#                                   u'[\u1f918]|'
#                                     u'[\u23e9]|'
#                                   u'[\u231a]|'
#                                   u'[\u3030]|'
#                                   u'[\ufe0f]|'
#                                   u'\uD83D[\uDE00-\uDE4F]|'
#                                   u'\uD83C[\uDDE0-\uDDFF]|'
#                                 u'[\u2702-\u27B0]|'
#                                   u'\uD83D[\uDC00-\uDDFF])+',
#                                   re.UNICODE)
#     text=myre.sub(' ', word)
#     if text!=' ' and text!='None':
#         # print(text)
#         return None
#     else:
#         return word
#
# print(skill_words_clean('ℂ'))
import json
a='{"favorited": false, "contributors": null}'
b='[{"favorited": false, "contributors": null}]'
c='{"favorited": false, "contributors": null'
a_dict=json.loads(a)
print('a',isinstance(a_dict,dict))

b_dict=json.loads(b)
print('b',isinstance(b_dict,dict))

try:
    c_dict=json.loads(c)
    print('c',isinstance(c_dict,dict))
except Exception as e:
    print('error:',e)
