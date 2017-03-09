package snippets.async;


import org.junit.Test;

import java.util.concurrent.CompletableFuture;

/**
 * Created by rnair on 3/7/17.
 */
public class TestAsync {

        @Test
        public void testAsyncThenApply(){
            CompletableFuture<String> promise = CompletableFuture.supplyAsync(() ->
                    {
                        System.out.println("Sub Process Started");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            System.out.println("Process Completed");
                        }
                        return "Completed";
                    }
            ).thenApply((v) -> {
                System.out.println(v + " in Applying");
                return v;
            }).thenApply((v) -> {
                System.out.println(v + " in Applying2");
                return v;
            });


            for(int i = 0; i < 10; i ++){
                System.out.println("Main Process Running");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
            }

        }



    @Test
    public void testAsyncThenApplyWithException(){
        CompletableFuture<String> promise = CompletableFuture.supplyAsync(() ->
                {
                    System.out.println("Sub Process Started");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    System.out.println("Sub process Completed");
                    return "Completed";
                }
        ).thenApply((v) -> {
            System.out.println(v + " in Applying");
            return v;
        }).thenApply((v) -> {
            System.out.println(v + " in Applying2");
            return v;
        }).exceptionally((v) -> {
            System.out.println("handling exception now");
            v.printStackTrace();
//            return v;
            return v.getMessage() ;
        });


        for(int i = 0; i < 10; i ++){
            System.out.println("Main Process Running");
            try {
                Thread.sleep(1000);
                if(i == 3 ){
                    System.out.println("Invoking Exception");
                    boolean result = promise.completeExceptionally(new Exception("Subprocess Invoking Exception"));
                    System.out.println(result);
                    promise.complete("Forced Complete");
                }
            } catch (InterruptedException e) {

            }
        }

    }

}


