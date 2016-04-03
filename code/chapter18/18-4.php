#!/usr/bin/php
<?php

$word2count = array();

// ݔ��Ϊ STDIN
while (($line = fgets(STDIN)) !== false) {
    // �Ƴ�����Ŀհ�
    $line = trim($line);
    // ÿһ�еĸ�ʽΪ (�� "tab" ����) ����¼��($word, $count)
    list($word, $count) = explode(chr(9), $line);
    // ת����ʽstring -> int
    $count = intval($count);
    // ���ܵ�Ƶ��
    if ($count > 0) $word2count[$word] += $count;
}

// ���в���Ҫ��������output���и�����
ksort($word2count);

// ���Y��д�� STDOUT (standard output)
foreach ($word2count as $word => $count) {
    echo $word, chr(9), $count, PHP_EOL;
}

?>
