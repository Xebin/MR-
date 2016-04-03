#!/usr/bin/php
<?php

$word2count = array();

//��׼����STDIN (standard input)
while (($line = fgets(STDIN)) !== false) {
   // �Ƴ�Сд��ո�
   $line = strtolower(trim($line));
   // �д�
   $words = preg_split('/\W/', $line, 0, PREG_SPLIT_NO_EMPTY);
   // ����+1
   foreach ($words as $word) {
       $word2count[$word] += 1;
   }
}

// �����д�� STDOUT (standard output)
foreach ($word2count as $word => $count) {
   echo $word, chr(9), $count, PHP_EOL;
}
?>