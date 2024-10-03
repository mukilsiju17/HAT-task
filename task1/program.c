#include<stdio.h>
int isPalindrome(char* a,int l,int r){
while (l<r){
if(a[l]!=a[r]){
return 0;
}
l++;
r--;
}
return 1;
}
void main()
{
char s[30];
printf("Enter a string: ");
scanf("%[^\n]%*c", s);
printf("%s",s);
int len = 1;
int n, ind, i, j = 0;
for(i=0;s[i]!='\0';i++){
n++;
}
for (i=0;i<n;i++){
for(j=n-1;j>i;j--){
if (isPalindrome(s,i,j)){
int temp = j-i+1;
if(temp>len){
len = temp;
ind = i;
}
break;
}
}
}
printf("\nLength of the longest palindromic substring is %d\n",len);
char res[len+1];
for(int i=0;i<len;i++){
res[i]=s[ind+i];
}
res[len] = '\0';
printf("The palindromic substring is: %s", res);
}
