from pprint import pprint
from unittest.mock import patch, MagicMock

from app.kafi_narrators import (
    extract_narrators, assign_narrator_id, getCombinations,
    compose_narrator_metadata, add_narrator_links, update_narrators,
    process_chapter_verses, process_chapter,
)
from app.lib_model import ProcessingReport
from app.models import Chapter, Language, PartType, Translation, Verse
from app.models.people import NarratorIndex, Narrator, ChainVerses
from app.models.quran import NarratorChain, SpecialText


def assert_text_narrators(text, narrators):
    v = Verse()
    v.text = [text]
    actual = extract_narrators(v)
    print('####################### EXPECTED')
    pprint(narrators)
    print('####################### ACTUAL')
    pprint(actual)
    assert actual == narrators

def test_1_1_1_1():
    assert_text_narrators(
        'أخْبَرَنَا أَبُو جَعْفَرٍ مُحَمَّدُ بْنُ يَعْقُوبَ قَالَ حَدَّثَنِي عِدَّةٌ مِنْ أَصْحَابِنَا مِنْهُمْ مُحَمَّدُ بْنُ يَحْيَى الْعَطَّارُ عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ عَنِ الْحَسَنِ بْنِ مَحْبُوبٍ عَنِ الْعَلَاءِ بْنِ رَزِينٍ عَنْ مُحَمَّدِ بْنِ مُسْلِمٍ عَنْ أَبِي جَعْفَرٍ ( عليه السلام ) قَالَ لَمَّا خَلَقَ اللَّهُ الْعَقْلَ اسْتَنْطَقَهُ ثُمَّ قَالَ لَهُ أَقْبِلْ فَأَقْبَلَ ثُمَّ قَالَ لَهُ أَدْبِرْ فَأَدْبَرَ ثُمَّ قَالَ وَ عِزَّتِي وَ جَلَالِي مَا خَلَقْتُ خَلْقاً هُوَ أَحَبُّ إِلَيَّ مِنْكَ وَ لَا أَكْمَلْتُكَ إِلَّا فِيمَنْ أُحِبُّ أَمَا إِنِّي إِيَّاكَ آمُرُ وَ إِيَّاكَ أَنْهَى وَ إِيَّاكَ أُعَاقِبُ وَ إِيَّاكَ أُثِيبُ .',
        [
            'أَبُو جَعْفَرٍ مُحَمَّدُ بْنُ يَعْقُوبَ',
            'عِدَّةٌ مِنْ أَصْحَابِنَا',
            'مُحَمَّدُ بْنُ يَحْيَى الْعَطَّارُ',
            'أَحْمَدَ بْنِ مُحَمَّدٍ',
            'الْحَسَنِ بْنِ مَحْبُوبٍ',
            'الْعَلَاءِ بْنِ رَزِينٍ',
            'مُحَمَّدِ بْنِ مُسْلِمٍ',
            'أَبِي جَعْفَرٍ ( عليه السلام )'
        ]
    )

def test_1_1_1_3():
    assert_text_narrators(
        'أَحْمَدُ بْنُ إِدْرِيسَ عَنْ مُحَمَّدِ بْنِ عَبْدِ الْجَبَّارِ عَنْ بَعْضِ أَصْحَابِنَا رَفَعَهُ إِلَى أَبِي عَبْدِ اللَّهِ ( عليه السلام ) قَالَ قُلْتُ لَهُ مَا الْعَقْلُ قَالَ مَا عُبِدَ بِهِ الرَّحْمَنُ وَ اكْتُسِبَ بِهِ الْجِنَانُ قَالَ قُلْتُ فَالَّذِي كَانَ فِي مُعَاوِيَةَ فَقَالَ تِلْكَ النَّكْرَاءُ تِلْكَ الشَّيْطَنَةُ وَ هِيَ شَبِيهَةٌ بِالْعَقْلِ وَ لَيْسَتْ بِالْعَقْلِ .',
        [
            "أَحْمَدُ بْنُ إِدْرِيسَ",
            "مُحَمَّدِ بْنِ عَبْدِ الْجَبَّارِ",
            "بَعْضِ أَصْحَابِنَا",
            "أَبِي عَبْدِ اللَّهِ ( عليه السلام )"
        ]
    )

def test_1_1_1_10():
    assert_text_narrators(
        'مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ عَنِ ابْنِ مَحْبُوبٍ عَنْ عَبْدِ اللَّهِ بْنِ سِنَانٍ قَالَ ذَكَرْتُ لِأَبِي عَبْدِ اللَّهِ ( عليه السلام ) رَجُلًا مُبْتَلًى بِالْوُضُوءِ وَ الصَّلَاةِ وَ قُلْتُ هُوَ رَجُلٌ عَاقِلٌ فَقَالَ أَبُو عَبْدِ اللَّهِ وَ أَيُّ عَقْلٍ لَهُ وَ هُوَ يُطِيعُ الشَّيْطَانَ فَقُلْتُ لَهُ وَ كَيْفَ يُطِيعُ الشَّيْطَانَ فَقَالَ سَلْهُ هَذَا الَّذِي يَأْتِيهِ مِنْ أَيِّ شَيْ\u200fءٍ هُوَ فَإِنَّهُ يَقُولُ لَكَ مِنْ عَمَلِ الشَّيْطَانِ .',
        [
            'مُحَمَّدُ بْنُ يَحْيَى',
            'أَحْمَدَ بْنِ مُحَمَّدٍ',
            'ابْنِ مَحْبُوبٍ',
            'عَبْدِ اللَّهِ بْنِ سِنَانٍ'
        ]
    )

def test_1_1_1_12():
    assert_text_narrators(
        'أَبُو عَبْدِ اللَّهِ الْأَشْعَرِيُّ عَنْ بَعْضِ أَصْحَابِنَا رَفَعَهُ عَنْ هِشَامِ بْنِ الْحَكَمِ قَالَ قَالَ لِي أَبُو الْحَسَنِ مُوسَى بْنُ جَعْفَرٍ ( عليه السلام ) يَا هِشَامُ إِنَّ اللَّهَ تَبَارَكَ وَ تَعَالَى بَشَّرَ أَهْلَ الْعَقْلِ وَ الْفَهْمِ فِي كِتَابِهِ فَقَالَ فَبَشِّرْ عِبادِ الَّذِينَ يَسْتَمِعُونَ الْقَوْلَ فَيَتَّبِعُونَ أَحْسَنَهُ أُولئِكَ الَّذِينَ هَداهُمُ اللَّهُ وَ أُولئِكَ هُمْ أُولُوا الْأَلْبابِ',
        [
            "أَبُو عَبْدِ اللَّهِ الْأَشْعَرِيُّ",
            "بَعْضِ أَصْحَابِنَا",
            "هِشَامِ بْنِ الْحَكَمِ"
        ]
    )

def test_1_1_1_13():
    assert_text_narrators(
        'عَلِيُّ بْنُ مُحَمَّدٍ عَنْ سَهْلِ بْنِ زِيَادٍ رَفَعَهُ قَالَ قَالَ أَمِيرُ الْمُؤْمِنِينَ ( عليه السلام ) الْعَقْلُ غِطَاءٌ سَتِيرٌ وَ الْفَضْلُ جَمَالٌ ظَاهِرٌ فَاسْتُرْ خَلَلَ خُلُقِكَ بِفَضْلِكَ وَ قَاتِلْ هَوَاكَ بِعَقْلِكَ تَسْلَمْ لَكَ الْمَوَدَّةُ وَ تَظْهَرْ لَكَ الْمَحَبَّةُ .',
        [
            "عَلِيُّ بْنُ مُحَمَّدٍ",
            "سَهْلِ بْنِ زِيَادٍ"
        ]
    )

def test_1_2_3_1():
    assert_text_narrators(
        'عَلِيُّ بْنُ مُحَمَّدٍ عَنْ سَهْلِ بْنِ زِيَادٍ وَ مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدِ بْنِ عِيسَى جَمِيعاً عَنِ ابْنِ مَحْبُوبٍ عَنْ أَبِي أُسَامَةَ عَنْ هِشَامِ بْنِ سَالِمٍ عَنْ أَبِي حَمْزَةَ عَنْ أَبِي إِسْحَاقَ السَّبِيعِيِّ عَمَّنْ حَدَّثَهُ مِمَّنْ يُوثَقُ بِهِ قَالَ سَمِعْتُ أَمِيرَ الْمُؤْمِنِينَ ( عليه السلام ) يَقُولُ إِنَّ النَّاسَ آلُوا بَعْدَ رَسُولِ اللَّهِ ( صلى الله عليه وآله ) إِلَى ثَلَاثَةٍ آلُوا إِلَى عَالِمٍ عَلَى هُدًى مِنَ اللَّهِ قَدْ أَغْنَاهُ اللَّهُ بِمَا عَلِمَ عَنْ عِلْمِ غَيْرِهِ وَ جَاهِلٍ مُدَّعٍ لِلْعِلْمِ لَا عِلْمَ لَهُ مُعْجَبٍ بِمَا عِنْدَهُ قَدْ فَتَنَتْهُ الدُّنْيَا وَ فَتَنَ غَيْرَهُ وَ مُتَعَلِّمٍ مِنْ عَالِمٍ عَلَى سَبِيلِ هُدًى مِنَ اللَّهِ وَ نَجَاةٍ ثُمَّ هَلَكَ مَنِ ادَّعَى وَ خَابَ مَنِ افْتَرَى .',
        [
            'عَلِيُّ بْنُ مُحَمَّدٍ',
            'سَهْلِ بْنِ زِيَادٍ',
            'مُحَمَّدُ بْنُ يَحْيَى',
            'أَحْمَدَ بْنِ مُحَمَّدِ بْنِ عِيسَى',
            'ابْنِ مَحْبُوبٍ',
            'أَبِي أُسَامَةَ',
            'هِشَامِ بْنِ سَالِمٍ',
            'أَبِي حَمْزَةَ',
            'أَبِي إِسْحَاقَ السَّبِيعِيِّ',
            'حَدَّثَهُ',
            'يُوثَقُ بِهِ'
        ]
    )

def test_1_2_1_9():
    assert_text_narrators(
        'عَلِيُّ بْنُ مُحَمَّدٍ عَنْ سَهْلِ بْنِ زِيَادٍ عَنْ مُحَمَّدِ بْنِ عِيسَى عَمَّنْ رَوَاهُ عَنْ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) قَالَ قَالَ لَهُ رَجُلٌ جُعِلْتُ فِدَاكَ رَجُلٌ عَرَفَ هَذَا الْأَمْرَ لَزِمَ بَيْتَهُ وَ لَمْ يَتَعَرَّفْ إِلَى أَحَدٍ مِنْ إِخْوَانِهِ قَالَ فَقَالَ كَيْفَ يَتَفَقَّهُ هَذَا فِي دِينِهِ ',
        [
            "عَلِيُّ بْنُ مُحَمَّدٍ",
            "سَهْلِ بْنِ زِيَادٍ",
            "مُحَمَّدِ بْنِ عِيسَى",
            "رَوَاهُ",
            "أَبِي عَبْدِ اللَّهِ ( عليه السلام )"
        ]
    )

def test_1_2_10_2():
    assert_text_narrators(
        'عِدَّةٌ مِنْ أَصْحَابِنَا عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ الْبَرْقِيِّ عَنْ أَبِيهِ عَنْ عَبْدِ اللَّهِ بْنِ الْمُغِيرَةِ وَ مُحَمَّدِ بْنِ سِنَانٍ عَنْ طَلْحَةَ بْنِ زَيْدٍ عَنْ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) فِي هَذِهِ الْآيَةِ وَ لا تُصَعِّرْ خَدَّكَ لِلنَّاسِ قَالَ لِيَكُنِ النَّاسُ عِنْدَكَ فِي الْعِلْمِ سَوَاءً .',
        [
            "عِدَّةٌ مِنْ أَصْحَابِنَا",
            "أَحْمَدَ بْنِ مُحَمَّدٍ الْبَرْقِيِّ",
            "أَبِيهِ",
            "عَبْدِ اللَّهِ بْنِ الْمُغِيرَةِ",
            "مُحَمَّدِ بْنِ سِنَانٍ",
            "طَلْحَةَ بْنِ زَيْدٍ",
            "أَبِي عَبْدِ اللَّهِ ( عليه السلام )"
        ]
    )

def test_1_2_19_2():
    assert_text_narrators(
        'الْحُسَيْنُ بْنُ مُحَمَّدٍ عَنْ مُعَلَّى بْنِ مُحَمَّدٍ عَنْ مُحَمَّدِ بْنِ جُمْهُورٍ الْعَمِّيِّ يَرْفَعُهُ قَالَ قَالَ رَسُولُ اللَّهِ ( صلى الله عليه وآله ) إِذَا ظَهَرَتِ الْبِدَعُ فِي أُمَّتِي فَلْيُظْهِرِ الْعَالِمُ عِلْمَهُ فَمَنْ لَمْ يَفْعَلْ فَعَلَيْهِ لَعْنَةُ اللَّهِ .',
        [
            "الْحُسَيْنُ بْنُ مُحَمَّدٍ",
            "مُعَلَّى بْنِ مُحَمَّدٍ",
            "مُحَمَّدِ بْنِ جُمْهُورٍ الْعَمِّيِّ"
        ]
    )

def test_1_3_1_1():
    assert_text_narrators(
        'أَخْبَرَنَا أَبُو جَعْفَرٍ مُحَمَّدُ بْنُ يَعْقُوبَ قَالَ حَدَّثَنِي عَلِيُّ بْنُ إِبْرَاهِيمَ بْنِ هَاشِمٍ عَنْ أَبِيهِ عَنِ الْحَسَنِ بْنِ إِبْرَاهِيمَ عَنْ يُونُسَ بْنِ عَبْدِ الرَّحْمَنِ عَنْ عَلِيِّ بْنِ مَنْصُورٍ قَالَ قَالَ لِي هِشَامُ بْنُ الْحَكَمِ كَانَ بِمِصْرَ زِنْدِيقٌ تَبْلُغُهُ عَنْ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) أَشْيَاءُ فَخَرَجَ إِلَى الْمَدِينَةِ لِيُنَاظِرَهُ فَلَمْ يُصَادِفْهُ بِهَا وَ قِيلَ لَهُ إِنَّهُ خَارِجٌ بِمَكَّةَ فَخَرَجَ إِلَى مَكَّةَ وَ نَحْنُ مَعَ أَبِي عَبْدِ اللَّهِ فَصَادَفَنَا وَ نَحْنُ مَعَ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) فِي الطَّوَافِ وَ كَانَ اسْمُهُ عَبْدَ الْمَلِكِ وَ كُنْيَتُهُ أَبُو عَبْدِ اللَّهِ',
        [
            'أَبُو جَعْفَرٍ مُحَمَّدُ بْنُ يَعْقُوبَ',
            'عَلِيُّ بْنُ إِبْرَاهِيمَ بْنِ هَاشِمٍ',
            'أَبِيهِ',
            'الْحَسَنِ بْنِ إِبْرَاهِيمَ',
            'يُونُسَ بْنِ عَبْدِ الرَّحْمَنِ',
            'عَلِيِّ بْنِ مَنْصُورٍ'
        ]
    )

def test_1_3_14_6():
    assert_text_narrators(
        'عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ عَنِ الْعَبَّاسِ بْنِ عَمْرٍو عَنْ هِشَامِ بْنِ الْحَكَمِ فِي حَدِيثِ الزِّنْدِيقِ الَّذِي سَأَلَ أَبَا عَبْدِ اللَّهِ ( عليه السلام ) فَكَانَ مِنْ سُؤَالِهِ أَنْ قَالَ لَهُ فَلَهُ رِضًا وَ سَخَطٌ فَقَالَ أَبُو عَبْدِ اللَّهِ ( عليه السلام ) نَعَمْ وَ لَكِنْ لَيْسَ ذَلِكَ عَلَى مَا يُوجَدُ مِنَ الْمَخْلُوقِينَ وَ ذَلِكَ أَنَّ الرِّضَا حَالٌ تَدْخُلُ عَلَيْهِ فَتَنْقُلُهُ مِنْ حَالٍ إِلَى حَالٍ لِأَنَّ الْمَخْلُوقَ أَجْوَفُ مُعْتَمِلٌ مُرَكَّبٌ لِلْأَشْيَاءِ فِيهِ مَدْخَلٌ وَ خَالِقُنَا لَا مَدْخَلَ لِلْأَشْيَاءِ فِيهِ لِأَنَّهُ وَاحِدٌ وَاحِدِيُّ الذَّاتِ وَاحِدِيُّ الْمَعْنَى فَرِضَاهُ ثَوَابُهُ وَ سَخَطُهُ عِقَابُهُ مِنْ غَيْرِ شَيْ\u200fءٍ يَتَدَاخَلُهُ فَيُهَيِّجُهُ وَ يَنْقُلُهُ مِنْ حَالٍ إِلَى حَالٍ لِأَنَّ ذَلِكَ مِنْ صِفَةِ الْمَخْلُوقِينَ الْعَاجِزِينَ الْمُحْتَاجِينَ .',
        [
            "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            "أَبِيهِ",
            "الْعَبَّاسِ بْنِ عَمْرٍو",
            "هِشَامِ بْنِ الْحَكَمِ"
        ]
    )

def test_1_4_108_44():
    assert_text_narrators(
        'وَ بِهَذَا الْإِسْنَادِ عَنْ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) فِي قَوْلِ اللَّهِ عَزَّ وَ جَلَّ وَ مَنْ يُرِدْ فِيهِ بِإِلْحادٍ بِظُلْمٍ قَالَ نَزَلَتْ فِيهِمْ حَيْثُ دَخَلُوا الْكَعْبَةَ فَتَعَاهَدُوا وَ تَعَاقَدُوا عَلَى كُفْرِهِمْ وَ جُحُودِهِمْ بِمَا نُزِّلَ فِي أَمِيرِ الْمُؤْمِنِينَ ( عليه السلام ) فَأَلْحَدُوا فِي الْبَيْتِ بِظُلْمِهِمُ الرَّسُولَ وَ وَلِيَّهُ فَبُعْداً لِلْقَوْمِ الظَّالِمِينَ .',
        [
            "بِهَذَا الْإِسْنَادِ",
            "أَبِي عَبْدِ اللَّهِ ( عليه السلام )"
        ]
    )

def test_2_1_30_5():
    assert_text_narrators(
        'عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ عَنِ ابْنِ أَبِي عُمَيْرٍ عَنْ زَيْدٍ الشَّحَّامِ عَنْ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) أَنَّ أَمِيرَ الْمُؤْمِنِينَ ( صلوات الله عليه )  جَلَسَ إِلَى حَائِطٍ مَائِلٍ يَقْضِي بَيْنَ النَّاسِ فَقَالَ بَعْضُهُمْ لَا تَقْعُدْ تَحْتَ هَذَا الْحَائِطِ فَإِنَّهُ مُعْوِرٌ فَقَالَ أَمِيرُ الْمُؤْمِنِينَ ( صلوات الله عليه )  حَرَسَ امْرَأً أَجَلُهُ فَلَمَّا قَامَ سَقَطَ الْحَائِطُ ',
        [
            'عَلِيُّ بْنُ إِبْرَاهِيمَ',
            'أَبِيهِ',
            'ابْنِ أَبِي عُمَيْرٍ',
            'زَيْدٍ الشَّحَّامِ',
            'أَبِي عَبْدِ اللَّهِ ( عليه السلام )'
        ]
    )

def test_2_1_121_6():
    assert_text_narrators(
        'عَنْهُ عَنْ إِسْمَاعِيلَ بْنِ مِهْرَانَ عَنْ سَيْفِ بْنِ عَمِيرَةَ عَمَّنْ سَمِعَ أَبَا عَبْدِ اللَّهِ ( عليه السلام ) يَقُولُ مَنْ كَفَّ غَضَبَهُ سَتَرَ اللَّهُ عَوْرَتَهُ .',
        [
            'عَنْهُ',
            'إِسْمَاعِيلَ بْنِ مِهْرَانَ',
            'سَيْفِ بْنِ عَمِيرَةَ',
            'أَبَا عَبْدِ اللَّهِ ( عليه السلام )'
        ]
    )

def test_2_2_60_33():
    assert_text_narrators(
        'عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ عَنِ ابْنِ أَبِي عُمَيْرٍ عَنْ مَنْصُورِ بْنِ يُونُسَ عَنْ أَبِي بَصِيرٍ عَنْ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) فَقَالَ قُلِ اللَّهُمَّ إِنِّي أَسْأَلُكَ قَوْلَ التَّوَّابِينَ وَ عَمَلَهُمْ وَ نُورَ الْأَنْبِيَاءِ وَ صِدْقَهُمْ وَ نَجَاةَ الْمُجَاهِدِينَ وَ ثَوَابَهُمْ وَ شُكْرَ الْمُصْطَفَيْنَ وَ نَصِيحَتَهُمْ وَ عَمَلَ الذَّاكِرِينَ وَ يَقِينَهُمْ وَ إِيمَانَ الْعُلَمَاءِ وَ فِقْهَهُمْ وَ تَعَبُّدَ الْخَاشِعِينَ وَ تَوَاضُعَهُمْ وَ حُكْمَ الْفُقَهَاءِ وَ سِيرَتَهُمْ وَ خَشْيَةَ الْمُتَّقِينَ وَ رَغْبَتَهُمْ وَ تَصْدِيقَ الْمُؤْمِنِينَ وَ تَوَكُّلَهُمْ وَ رَجَاءَ الْمُحْسِنِينَ وَ بِرَّهُمْ',
        [
            'عَلِيُّ بْنُ إِبْرَاهِيمَ',
            'أَبِيهِ',
            'ابْنِ أَبِي عُمَيْرٍ',
            'مَنْصُورِ بْنِ يُونُسَ',
            'أَبِي بَصِيرٍ',
            'أَبِي عَبْدِ اللَّهِ ( عليه السلام )'
        ]
    )

def test_3_3_82_2():
    assert_text_narrators(
        'عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ عَنْ عَمْرِو بْنِ عُثْمَانَ عَنْ أَبِي جَمِيلَةَ عَنْ جَابِرٍ عَنْ أَبِي جَعْفَرٍ ( عليه السلام ) مِثْلَهُ .',
        [
            'عَلِيُّ بْنُ إِبْرَاهِيمَ',
            'أَبِيهِ',
            'عَمْرِو بْنِ عُثْمَانَ',
            'أَبِي جَمِيلَةَ',
            'جَابِرٍ',
            'أَبِي جَعْفَرٍ ( عليه السلام )',
        ]
    )

def test_4_2_6_8():
    assert_text_narrators(
        'عِدَّةٌ مِنْ أَصْحَابِنَا عَنْ أَحْمَدَ بْنِ مُحَمَّدِ بْنِ عِيسَى عَنْ حَمْزَةَ أَبِي يَعْلَى عَنْ مُحَمَّدِ بْنِ الْحَسَنِ بْنِ أَبِي خَالِدٍ رَفَعَهُ عَنْ أَبِي عَبْدِ اللَّهِ ( عليه السلام ) إِذَا صَحَّ هِلَالُ شَهْرِ رَجَبٍ فَعُدَّ تِسْعَةً وَ خَمْسِينَ يَوْماً وَ صُمْ يَوْمَ السِّتِّينَ .',
        [
            'عِدَّةٌ مِنْ أَصْحَابِنَا',
            'أَحْمَدَ بْنِ مُحَمَّدِ بْنِ عِيسَى',
            'حَمْزَةَ أَبِي يَعْلَى',
            'مُحَمَّدِ بْنِ الْحَسَنِ بْنِ أَبِي خَالِدٍ',
            'أَبِي عَبْدِ اللَّهِ ( عليه السلام )'
        ]
    )

def test_6_6_48_17():
    assert_text_narrators(
        'مُحَمَّدُ بْنُ يَحْيَى عَنْ عَلِيِّ بْنِ إِبْرَاهِيمَ الْجَعْفَرِيِّ عَنْ مُحَمَّدِ بْنِ الْفُضَيْلِ رَفَعَهُ عَنْهُمْ ( عليهم السلام ) قَالُوا كَانَ النَّبِيُّ ( صلى الله عليه وآله ) إِذَا أَكَلَ لَقَّمَ مَنْ بَيْنَ عَيْنَيْهِ وَ إِذَا شَرِبَ سَقَى مَنْ عَلَى يَمِينِهِ .',
        [
            'مُحَمَّدُ بْنُ يَحْيَى',
            'عَلِيِّ بْنِ إِبْرَاهِيمَ الْجَعْفَرِيِّ',
            'مُحَمَّدِ بْنِ الْفُضَيْلِ',
            'عَنْهُمْ ( عليهم السلام )',
        ]
    )

def test_8_1_5_1():
    assert_text_narrators(
        '14455-مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ عَنْ بَعْضِ أَصْحَابِهِ وَ عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ عَنِ ابْنِ أَبِي عُمَيْرٍ جَمِيعاً عَنْ مُحَمَّدِ بْنِ أَبِي حَمْزَةَ عَنْ حُمْرَانَ قَالَ قَالَ أَبُو عَبْدِ اللَّهِ ( عليه السلام ) وَ ذُكِرَ هَؤُلَاءِ عِنْدَهُ وَ سُوءُ حَالِ الشِّيعَةِ عِنْدَهُمْ ',
        [
            '14455-مُحَمَّدُ بْنُ يَحْيَى',
            'أَحْمَدَ بْنِ مُحَمَّدٍ',
            'بَعْضِ أَصْحَابِهِ',
            'عَلِيُّ بْنُ إِبْرَاهِيمَ',
            'أَبِيهِ',
            'ابْنِ أَبِي عُمَيْرٍ',
            'مُحَمَّدِ بْنِ أَبِي حَمْزَةَ',
            'حُمْرَانَ',
        ]
    )

def test_8_1_13_1():
    assert_text_narrators(
        '14475-\xa0 أَبُو عَلِيٍّ الْأَشْعَرِيُّ عَنْ مُحَمَّدِ بْنِ سَالِمٍ وَ عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ جَمِيعاً عَنْ أَحْمَدَ بْنِ النَّضْرِ وَ مُحَمَّدُ بْنُ يَحْيَى عَنْ مُحَمَّدِ بْنِ أَبِي الْقَاسِمِ عَنِ الْحُسَيْنِ بْنِ أَبِي قَتَادَةَ جَمِيعاً عَنْ عَمْرِو بْنِ شِمْرٍ عَنْ جَابِرٍ عَنْ أَبِي جَعْفَرٍ ( عليه السلام ) قَالَ خَرَجَ رَسُولُ اللَّهِ ( صلى الله عليه وآله ) لِعَرْضِ الْخَيْلِ فَمَرَّ بِقَبْرِ أَبِي أُحَيْحَةَ فَقَالَ أَبُو بَكْرٍ لَعَنَ اللَّهُ صَاحِبَ هَذَا الْقَبْرِ فَوَ اللَّهِ إِنْ كَانَ لَيَصُدُّ عَنْ سَبِيلِ اللَّهِ وَ يُكَذِّبُ رَسُولَ اللَّهِ ( صلى الله عليه وآله ) فَقَالَ خَالِدٌ ابْنُهُ بَلْ لَعَنَ اللَّهُ أَبَا قُحَافَةَ فَوَ اللَّهِ مَا كَانَ يُقْرِي الضَّيْفَ وَ لَا يُقَاتِلُ الْعَدُوَّ فَلَعَنَ اللَّهُ أَهْوَنَهُمَا عَلَى الْعَشِيرَةِ فَقْداً فَأَلْقَى رَسُولُ اللَّهِ ( صلى الله عليه وآله ) خِطَامَ رَاحِلَتِهِ عَلَى غَارِبِهَا ثُمَّ قَالَ إِذَا أَنْتُمْ تَنَاوَلْتُمُ الْمُشْرِكِينَ فَعُمُّوا وَ لَا تَخُصُّوا فَيَغْضَبَ وُلْدُهُ',
        [
            'أَبُو عَلِيٍّ الْأَشْعَرِيُّ',
            'مُحَمَّدِ بْنِ سَالِمٍ',
            'عَلِيُّ بْنُ إِبْرَاهِيمَ',
            'أَبِيهِ',
            'أَحْمَدَ بْنِ النَّضْرِ',
            'مُحَمَّدُ بْنُ يَحْيَى',
            'مُحَمَّدِ بْنِ أَبِي الْقَاسِمِ',
            'الْحُسَيْنِ بْنِ أَبِي قَتَادَةَ',
            'عَمْرِو بْنِ شِمْرٍ',
            'جَابِرٍ',
            'أَبِي جَعْفَرٍ ( عليه السلام )'
        ]
    )


class TestAssignNarratorId:
    """Test narrator ID assignment"""

    def test_assign_new_narrator(self):
        """Test ID assignment for new narrator"""
        narrator_index = NarratorIndex()
        narrator_index.name_id = {}
        narrator_index.id_name = {}
        narrator_index.last_id = 0

        ids = assign_narrator_id(["مُحَمَّدُ بْنُ يَحْيَى"], narrator_index)
        assert ids == [1]
        assert narrator_index.last_id == 1
        assert narrator_index.id_name[1] == "مُحَمَّدُ بْنُ يَحْيَى"

    def test_assign_existing_narrator(self):
        """Test ID reuse for existing narrator"""
        narrator_index = NarratorIndex()
        narrator_index.name_id = {"مُحَمَّدُ": 5}
        narrator_index.id_name = {5: "مُحَمَّدُ"}
        narrator_index.last_id = 5

        ids = assign_narrator_id(["مُحَمَّدُ"], narrator_index)
        assert ids == [5]
        assert narrator_index.last_id == 5  # Unchanged

    def test_assign_multiple_narrators(self):
        """Test ID assignment for multiple narrators"""
        narrator_index = NarratorIndex()
        narrator_index.name_id = {}
        narrator_index.id_name = {}
        narrator_index.last_id = 0

        ids = assign_narrator_id(["أَحْمَدُ", "مُحَمَّدُ"], narrator_index)
        assert ids == [1, 2]
        assert narrator_index.last_id == 2


class TestGetCombinations:
    """Test narrator chain combination generation"""

    def test_two_narrators(self):
        """Test combination generation for 2 narrators"""
        result = getCombinations([1, 2])

        # Expected: {1: [("1-2", [1,2])], 2: [("1-2", [1,2])]}
        assert 1 in result
        assert 2 in result
        assert "1-2" in [key for key, _ in result[1]]
        assert "1-2" in [key for key, _ in result[2]]

    def test_three_narrators(self):
        """Test subchain combinations for 3 narrators"""
        result = getCombinations([1, 2, 3])

        # Should generate: 1-2, 1-2-3, 2-3
        narrator_1_chains = [key for key, _ in result[1]]
        assert "1-2" in narrator_1_chains
        assert "1-2-3" in narrator_1_chains

    def test_single_narrator(self):
        """Test no combinations for single narrator"""
        result = getCombinations([1])
        assert result == {}

    def test_five_narrators_only_full_chain_and_pairs(self):
        """Optimized getCombinations generates full chain + direct pairs only.

        For [1,2,3,4,5]: full chain "1-2-3-4-5" + pairs "1-2","2-3","3-4","4-5".
        Old implementation would also generate "1-2-3", "2-3-4", "3-4-5", etc.
        """
        result = getCombinations([1, 2, 3, 4, 5])

        # All 5 narrators should be present
        for n in [1, 2, 3, 4, 5]:
            assert n in result

        # Collect all unique subchain keys across all narrators
        all_keys = set()
        for entries in result.values():
            for key, _ in entries:
                all_keys.add(key)

        # Expected: full chain + 4 consecutive pairs = 5 keys
        assert all_keys == {"1-2-3-4-5", "1-2", "2-3", "3-4", "4-5"}

        # Intermediate subsequences should NOT be present
        assert "1-2-3" not in all_keys
        assert "2-3-4" not in all_keys
        assert "3-4-5" not in all_keys
        assert "1-2-3-4" not in all_keys
        assert "2-3-4-5" not in all_keys

    def test_two_narrators_no_duplicate_entries(self):
        """When chain has exactly 2 narrators, full chain equals the pair.

        Should produce only 1 entry per narrator, not 2 duplicates.
        """
        result = getCombinations([1, 2])

        # Each narrator should have exactly 1 entry (the full chain which is also the pair)
        assert len(result[1]) == 1
        assert len(result[2]) == 1
        assert result[1][0][0] == "1-2"
        assert result[2][0][0] == "1-2"

    def test_empty_list(self):
        """Empty list produces no combinations"""
        result = getCombinations([])
        assert result == {}

class TestComposeNarratorMetadata:
    """Test narrator metadata composition"""

    def test_basic_metadata(self):
        """Test metadata for narrator with basic info"""
        narrator = Narrator()
        narrator.index = 1
        narrator.path = "/people/narrators/1"
        narrator.verse_paths = {"/books/al-kafi:1:1:1", "/books/al-kafi:1:1:2"}
        narrator.subchains = {}

        # Add subchains
        cv1 = ChainVerses()
        cv1.narrator_ids = [1, 2]
        cv1.verse_paths = {"/books/al-kafi:1:1:1"}
        narrator.subchains["1-2"] = cv1

        cv2 = ChainVerses()
        cv2.narrator_ids = [3, 1]
        cv2.verse_paths = {"/books/al-kafi:1:1:2"}
        narrator.subchains["3-1"] = cv2

        metadata = compose_narrator_metadata("مُحَمَّدُ", narrator)

        assert metadata["titles"]["ar"] == "مُحَمَّدُ"
        assert metadata["narrations"] == 2
        assert metadata["narrated_to"] == 1  # Chain 1-2
        assert metadata["narrated_from"] == 1  # Chain 3-1


class TestAddNarratorLinks:
    """Test narrator link injection into hadith narrator_chain"""

    def test_basic_narrator_links(self):
        """Test that narrator names are linked in narrator_chain.parts"""
        hadith = Verse()
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.narrator_chain = NarratorChain()
        hadith.narrator_chain.text = "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ"
        hadith.narrator_chain.parts = []

        narrator_index = NarratorIndex()
        narrator_index.name_id = {
            "مُحَمَّدُ بْنُ يَحْيَى": 1,
            "أَحْمَدَ بْنِ مُحَمَّدٍ": 2,
        }
        narrator_index.id_name = {
            1: "مُحَمَّدُ بْنُ يَحْيَى",
            2: "أَحْمَدَ بْنِ مُحَمَّدٍ",
        }

        add_narrator_links(hadith, [1, 2], narrator_index)

        # Should have narrator parts with kind="narrator" and path
        narrator_parts = [p for p in hadith.narrator_chain.parts if p.kind == "narrator"]
        assert len(narrator_parts) == 2
        assert narrator_parts[0].path == "/people/narrators/1"
        assert narrator_parts[1].path == "/people/narrators/2"

    def test_no_chain_text_is_noop(self):
        """Test that missing narrator_chain doesn't crash"""
        hadith = Verse()
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.narrator_chain = None

        add_narrator_links(hadith, [1], NarratorIndex())
        # Should not crash, narrator_chain stays None

    def test_plain_parts_between_narrators(self):
        """Test that text between narrators gets plain parts"""
        hadith = Verse()
        hadith.path = "/books/test:1"
        hadith.narrator_chain = NarratorChain()
        hadith.narrator_chain.text = "أَوَّلُ عَنْ ثَانِي"
        hadith.narrator_chain.parts = []

        narrator_index = NarratorIndex()
        narrator_index.id_name = {1: "أَوَّلُ", 2: "ثَانِي"}

        add_narrator_links(hadith, [1, 2], narrator_index)

        plain_parts = [p for p in hadith.narrator_chain.parts if p.kind == "plain"]
        narrator_parts = [p for p in hadith.narrator_chain.parts if p.kind == "narrator"]
        # Should have connector text " عَنْ " as plain, plus trailing plain
        assert len(narrator_parts) == 2
        assert any(" عَنْ " in p.text for p in plain_parts)


class TestUpdateNarrators:
    """Test narrator verse tracking and subchain building"""

    def test_update_narrators_tracks_verse_path(self):
        """Test that narrator.verse_paths gets the hadith path added"""
        hadith = Verse()
        hadith.path = "/books/al-kafi:1:1:1:1"

        narrator_index = NarratorIndex()
        narrator_index.name_id = {"A": 1, "B": 2}
        narrator_index.id_name = {1: "A", 2: "B"}
        narrator_index.last_id = 2

        narrators = {}
        narrator1 = Narrator()
        narrator1.index = 1
        narrator1.path = "/people/narrators/1"
        narrator1.titles = {"ar": "A"}
        narrator1.verse_paths = set()
        narrator1.subchains = {}
        narrators[1] = narrator1

        narrator2 = Narrator()
        narrator2.index = 2
        narrator2.path = "/people/narrators/2"
        narrator2.titles = {"ar": "B"}
        narrator2.verse_paths = set()
        narrator2.subchains = {}
        narrators[2] = narrator2

        update_narrators(hadith, [1, 2], narrators, narrator_index)

        assert hadith.path in narrators[1].verse_paths
        assert hadith.path in narrators[2].verse_paths

    def test_update_narrators_creates_subchains(self):
        """Test that subchains are created between narrator pairs"""
        hadith = Verse()
        hadith.path = "/books/test:1:1"

        narrator_index = NarratorIndex()
        narrator_index.name_id = {"X": 1, "Y": 2, "Z": 3}
        narrator_index.id_name = {1: "X", 2: "Y", 3: "Z"}

        narrators = {}
        for i in range(1, 4):
            n = Narrator()
            n.index = i
            n.path = f"/people/narrators/{i}"
            n.titles = {"ar": narrator_index.id_name[i]}
            n.verse_paths = set()
            n.subchains = {}
            narrators[i] = n

        update_narrators(hadith, [1, 2, 3], narrators, narrator_index)

        # Narrator 1 should have subchain "1-2" and "1-2-3"
        assert "1-2" in narrators[1].subchains
        assert "1-2-3" in narrators[1].subchains

        # Narrator 2 should have subchains "1-2", "1-2-3", "2-3"
        assert "2-3" in narrators[2].subchains


class TestComposeNarratorMetadataExtended:
    """Extended tests for narrator metadata composition"""

    def test_narrator_with_no_subchains(self):
        """Test metadata for narrator without any subchains"""
        narrator = Narrator()
        narrator.index = 1
        narrator.path = "/people/narrators/1"
        narrator.verse_paths = {"/books/test:1"}
        narrator.subchains = {}

        metadata = compose_narrator_metadata("test", narrator)
        assert metadata["narrations"] == 1
        assert metadata["narrated_to"] == 0
        assert metadata["narrated_from"] == 0

    def test_narrator_with_long_chains_only(self):
        """Test that only length-2 chains count for narrated_to/from"""
        narrator = Narrator()
        narrator.index = 1
        narrator.path = "/people/narrators/1"
        narrator.verse_paths = {"/books/test:1"}
        narrator.subchains = {}

        cv = ChainVerses()
        cv.narrator_ids = [1, 2, 3]  # Length 3, not 2
        cv.verse_paths = {"/books/test:1"}
        narrator.subchains["1-2-3"] = cv

        metadata = compose_narrator_metadata("test", narrator)
        assert metadata["narrated_to"] == 0
        assert metadata["narrated_from"] == 0


def _make_narrator_index():
    """Helper to create a fresh NarratorIndex."""
    ni = NarratorIndex()
    ni.name_id = {}
    ni.id_name = {}
    ni.last_id = 0
    return ni


class TestProcessChapterVerses:
    """Test kafi_narrators.process_chapter_verses end-to-end processing"""

    def test_extracts_narrators_and_builds_chain(self):
        """Test that process_chapter_verses extracts narrators and builds chain parts"""
        narrator_index = _make_narrator_index()
        narrators = {}

        chapter = Chapter()
        chapter.verses = []

        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.text = [
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ test text"
        ]
        hadith.translations = {}
        chapter.verses.append(hadith)

        process_chapter_verses(chapter, narrator_index, narrators)

        # Narrator chain should be populated (text is set to None for optimization)
        assert hadith.narrator_chain is not None
        assert hadith.narrator_chain.text is None
        # Narrator parts should have been created
        narrator_parts = [p for p in hadith.narrator_chain.parts if p.kind == "narrator"]
        assert len(narrator_parts) >= 2
        # Narrators should be tracked
        assert len(narrators) >= 2

    def test_skips_empty_text(self):
        """Test that hadiths with empty text are skipped"""
        narrator_index = _make_narrator_index()
        narrators = {}

        chapter = Chapter()
        chapter.verses = []

        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-kafi:7:3:15:5"
        hadith.text = []  # Empty text
        hadith.translations = {}
        chapter.verses.append(hadith)

        # Should not crash
        process_chapter_verses(chapter, narrator_index, narrators)
        assert len(narrators) == 0

    def test_strips_span_tags(self):
        """Test that HTML span tags are stripped from text before processing"""
        narrator_index = _make_narrator_index()
        narrators = {}

        chapter = Chapter()
        chapter.verses = []

        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1"
        hadith.text = [
            '<span class="x">مُحَمَّدُ بْنُ يَحْيَى</span> عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ text'
        ]
        hadith.translations = {}
        chapter.verses.append(hadith)

        process_chapter_verses(chapter, narrator_index, narrators)

        # Span tags should have been removed before narrator extraction
        assert "<span" not in hadith.text[0]

    def test_multiple_hadiths_share_narrator_ids(self):
        """Test that the same narrator name across hadiths gets the same ID"""
        narrator_index = _make_narrator_index()
        narrators = {}

        chapter = Chapter()
        chapter.verses = []

        for i in range(1, 3):
            hadith = Verse()
            hadith.part_type = PartType.Hadith
            hadith.path = f"/books/test:{i}"
            hadith.text = [
                "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ text"
            ]
            hadith.translations = {}
            chapter.verses.append(hadith)

        process_chapter_verses(chapter, narrator_index, narrators)

        # Both hadiths should reference the same narrator IDs
        assert narrator_index.last_id == 2  # Only 2 unique narrators


class TestProcessChapter:
    """Test kafi_narrators.process_chapter recursive traversal"""

    def test_recurses_into_subchapters(self):
        """Test that process_chapter recurses into nested chapters"""
        narrator_index = _make_narrator_index()
        narrators = {}

        book = Chapter()
        book.chapters = []

        ch = Chapter()
        ch.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1:1"
        hadith.text = [
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ text"
        ]
        hadith.translations = {}
        ch.verses.append(hadith)
        book.chapters.append(ch)

        result = process_chapter(book, narrator_index, narrators)

        # Should have processed the hadith
        assert len(narrators) >= 2
        assert result is narrators

    def test_handles_empty_chapter(self):
        """Test that empty chapters (no verses or subchapters) don't crash"""
        narrator_index = _make_narrator_index()
        narrators = {}

        empty = Chapter()
        result = process_chapter(empty, narrator_index, narrators)
        assert result is narrators
        assert len(narrators) == 0

    def test_deeply_nested_chapters(self):
        """Test recursion through multiple levels of nesting"""
        narrator_index = _make_narrator_index()
        narrators = {}

        # Build: root -> level1 -> level2 -> hadith
        root = Chapter()
        root.chapters = []

        level1 = Chapter()
        level1.chapters = []

        level2 = Chapter()
        level2.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1:1:1"
        hadith.text = [
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ text"
        ]
        hadith.translations = {}
        level2.verses.append(hadith)
        level1.chapters.append(level2)
        root.chapters.append(level1)

        process_chapter(root, narrator_index, narrators)

        # Hadith at depth 3 should still be processed
        assert len(narrators) >= 2
        assert hadith.narrator_chain is not None


class TestProcessingReportIntegration:
    """Test that ProcessingReport is correctly used by kafi_narrators functions."""

    def test_extract_narrators_increments_report_on_no_match(self):
        """extract_narrators increments report counter when no narrators found."""
        report = ProcessingReport()

        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1"
        hadith.text = ["text without any narrator patterns"]

        result = extract_narrators(hadith, report)

        assert result == []
        assert report.narrations_without_narrators == 1

    def test_extract_narrators_does_not_increment_on_match(self):
        """extract_narrators does not increment counter when narrators are found."""
        report = ProcessingReport()

        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1"
        hadith.text = ["مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ text"]

        result = extract_narrators(hadith, report)

        assert len(result) > 0
        assert report.narrations_without_narrators == 0

    def test_process_chapter_passes_report_through(self):
        """process_chapter passes report to extract_narrators for no-match counting."""
        report = ProcessingReport()
        narrator_index = NarratorIndex()
        narrator_index.name_id = {}
        narrator_index.id_name = {}
        narrator_index.last_id = 0
        narrators = {}

        chapter = Chapter()
        chapter.verses = []

        # Hadith with no narrator pattern
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1"
        hadith.text = ["plain text without narrators"]
        hadith.translations = {}
        chapter.verses.append(hadith)

        process_chapter(chapter, narrator_index, narrators, report)

        assert report.narrations_without_narrators == 1
