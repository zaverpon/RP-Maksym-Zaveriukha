/* (c) 2017 Tomas Plachetka
 * <pc> znamena uspesnost z cviceni v percentach
 * <ps> znamena uspesnost z pisomnej skusky v percentach */

/* Stupnica sa vypocita z
 * E+k*E+k^2*E+k^3*E+k^4*E=50,
 * kde E je dlzka E-ckoveho pasma. Ak E=5, tak k=1.353. Z toho vyjde: 
 * 50-55  E
 * 56-61  D
 * 62-70  C
 * 71-83  B
 * 84-100 A
*/

#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    int ps, pc;

    if (argc != 3)
    {
      printf("Pouzitie: %s <pc> <ps>\n", argv[0]);
      printf("Priklad pouzitia: %s 44 60\n", argv[0]);
      exit(1);
    }
    
    ps = atoi(argv[1]);
    pc = atoi(argv[2]);
    
    if (! ((pc >= 45) && (ps >= 45) && (pc + ps >= 100)))
      printf("Fx\n");
    else
    {
      if ((pc + ps) / 2 <= 55)
        printf("E\n");
      else
      {
        if ((pc + ps) / 2 <= 61)
          printf("D\n");
        else 
        {
          if ((pc + ps) / 2 <= 70)
            printf("C\n");
          else
          {
            if ((pc + ps) / 2 <= 83)
              printf("B\n");
            else
              printf("A\n");
          }
        }
      }
    }

    return 0;
}