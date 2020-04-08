def map_geography(region):
    for key in geography.keys():
        if region in key:
            return geography[key]
        else:
            raise ValueError("no match geography")


geography = {
    (
        '강원', '강원강릉', '강원삼척', '강원양양',
        '강원원주', '강원정선', '강원춘천', '강원평창',
        '강원홍천', '강원횡성'
    ): "강원도",
    (
        '서울', '서울서부', '인천', '제주',
        '경기', '경기수원', '경기안성', '경기양주',
        '경기양평', '경기의정부', '경기이천', '경기파주',
        '경기평택', '경기포천', '경기화성'
    ): "서울경기",
    (
        '경남거창', '경남고성', '경남남해', '경남사천', '경남양산', '경남울산',
        '경남진주', '경남창녕', '경남창원', '경남하동', '경남함안', '경남함양', '경남합천', '경북경산', '경북경주',
        '경북고령', '경북구미', '경북김천', '경북봉화', '경북상주', '경북안동', '경북영양', '경북영주', '경북영천',
        '경북예천', '경북의성', '경북청도', '경북청송', '경북포항',
        '대구', '부산', '울산'
    ): "경상도",
    (
        '전남강진', '전남고흥', '전남나주', '전남담양', '전남무안',
        '전남순천', '전남영광', '전남장성', '전남함평', '전남화순', '전북김제', '전북남원', '전북부안', '전북순창',
        '전북임실', '전북장수', '전북전주', '전북정읍', '전주', '광주'
    ): "전라도",
    (
        '충남공주', '충남논산',
        '충남부여', '충남서산', '충남아산', '충남예산', '충남천안', '충남홍성', '충북보은', '충북영동', '충북옥천',
        '충북음성', '충북제천', '충북청주', '충북충주',
        '청주', '대전', '세종'
    ): "충청도"
}