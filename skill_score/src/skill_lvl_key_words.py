"""
level = u"熟练|熟悉|精通|了解|擅长|熟习|知道|理解|熟知|参与|具有|擅长|具备|掌握"
"""
from collections import Counter
import re
import numpy as np

level_grade = {"精通": 1,
               "擅长": 2,
               "掌握": 3,
               "熟知": 4,
               "熟练": 5,
               "熟悉": 6,
               "熟习": 7,
               "具备": 8,
               "具有": 9,
               "理解": 10,
               "参与": 11,
               "了解": 12,
               "知道": 13}

level_score = {"精通": 100,
               "擅长": 90,
               "掌握": 90,
               "熟知": 85,
               "熟练": 85,
               "熟悉": 80,
               "熟习": 80,
               "具备": 70,
               "具有": 70,
               "理解": 60,
               "参与": 60,
               "了解": 50,
               "知道": 50}


def level_score_(skill_level):
    assert isinstance(skill_level, str)
    level = u"(熟练|熟悉|精通|了解|擅长|熟习|知道|理解|熟知|参与|具有|具备|掌握)"
    rule = re.compile(level)
    level_list = rule.split(skill_level)
    level_list_soce = [level_score.get(x) for x in level_list if x in level_score.keys()]
    if level_list_soce:
        level_list_soce_mean = np.round(np.mean(level_list_soce), 2)
    else:
        level_list_soce_mean = 0
    # print(level_list_soce_mean)
    return level_list_soce_mean


def level_grade_(skill_level):
    assert isinstance(skill_level, str)
    level = u"(熟练|熟悉|精通|了解|擅长|熟习|知道|理解|熟知|参与|具有|具备|掌握)"
    rule = re.compile(level)
    level_list = rule.split(skill_level)
    level_list_grade = [level_grade.get(x) for x in level_list if x in level_grade.keys()]
    level_grade_reverse = {v: k for k, v in level_grade.items()}
    max_grade = level_grade_reverse.get(min(level_list_grade))
    return max_grade


if __name__ == '__main__':
    print(level_score_("熟练掌握理解"))
    print(level_grade_("熟练掌握理解"))
